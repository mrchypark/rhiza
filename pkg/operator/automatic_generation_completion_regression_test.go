package operator

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// TestAutomaticFencedGenerationCompletesAndInstallsInventory continues from an
// installed generation, loses its quorum, and drives the newly fenced child
// through the real recovery reconciler before CONTROL installs its inventory.
func TestAutomaticFencedGenerationCompletesAndInstallsInventory(t *testing.T) {
	s := completedAutomaticTarget(t, voterMembers(3))
	if err := s.c.advanceAutomatic(s.ctx, s.r, s.state, s.version, s.api.sts); err != nil {
		t.Fatalf("install initial completed generation: %v", err)
	}
	state, version, err := s.c.loadAuto(s.ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	if state.Completed == nil || state.Intent != nil || len(state.Voters) != 3 {
		t.Fatalf("initial target was not installed before the next incident: %+v", state)
	}
	previousClusterID := state.ActiveClusterID

	s.r.Spec.Automatic = true
	s.r.Spec.Policy = &AutoPolicy{FailureGraceSeconds: 1, CooldownSeconds: 1, MaxRecoveriesPerDay: 10}
	state.BindingHash = autoBinding(s.r)
	state.SuspectedSince = time.Now().Add(-time.Hour)
	state.Completions = []time.Time{state.SuspectedSince}
	if err := s.c.saveAuto(s.ctx, s.r, state, version); err != nil {
		t.Fatal(err)
	}

	api := &automaticGenerationRecoveryAPI{controller: s.c, cluster: s.r, peers: s.api, secrets: map[string]object{}}
	if s.api.secret != nil {
		name := str(nested(s.api.secret, "metadata", "name"))
		api.secrets[name] = s.api.secret
	}
	server := httptest.NewTLSServer(api)
	t.Cleanup(server.Close)
	s.c.Kube.BaseURL, s.c.Kube.Client = server.URL, server.Client()

	fenceCalls := 0
	s.c.Fencer = &completionOutcomeFencer{
		countingFencer:              &countingFencer{count: &fenceCalls},
		archiveForkRegistrarFixture: &archiveForkRegistrarFixture{},
	}
	s.api.noQuorum = true
	if err := s.c.reconcileCluster(s.ctx, s.r); err != nil {
		t.Fatalf("reserve generation recovery after quorum loss: %v", err)
	}
	state, _, err = s.c.loadAuto(s.ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent == nil || state.Intent.Request.Scope != ScopeGeneration || state.Intent.Proof != nil {
		t.Fatalf("quorum loss did not reserve an unfenced generation intent: %+v", state.Intent)
	}

	if err := s.c.reconcileCluster(s.ctx, s.r); err != nil {
		t.Fatalf("persist generation fence: %v", err)
	}
	state, _, err = s.c.loadAuto(s.ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent == nil || state.Intent.Proof == nil || fenceCalls != 1 {
		t.Fatalf("generation fence was not persisted exactly once: calls=%d intent=%+v", fenceCalls, state.Intent)
	}

	if err := s.c.reconcileCluster(s.ctx, s.r); err != nil {
		t.Fatalf("create and pin generation child: %v", err)
	}
	state, _, err = s.c.loadAuto(s.ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent == nil || state.Intent.RecoveryUID == "" || api.child == nil ||
		state.Intent.RecoveryUID != str(api.child.Metadata["uid"]) || state.Intent.SpecHash != hashJSON(api.child.Spec) {
		t.Fatalf("generation child was not pinned to CONTROL before execution: intent=%+v child=%+v", state.Intent, api.child)
	}
	pinnedIntent := *state.Intent

	s.api.mu.Lock()
	s.api.recoveryStatusClusterID = "wrong-generation-cluster"
	s.api.mu.Unlock()
	for step := 0; step < 24 && api.child.Status.Stage != "WaitingReady" && api.child.Status.Stage != "Complete"; step++ {
		if err := s.c.reconcile(s.ctx, api.child); err != nil {
			t.Fatalf("reconcile generation child step %d (stage %q): %v", step, api.child.Status.Stage, err)
		}
	}
	if api.child.Status.Stage != "Restarted" || api.child.Status.Phase != "WaitingReady" {
		t.Fatalf("wrong recovery status did not hold the child at the target-quorum readiness gate: %+v", api.child.Status)
	}
	stateBeforeInvalidStatus, _, err := s.c.loadAuto(s.ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	previousCompletionHash := hashJSON(stateBeforeInvalidStatus.Completed)
	previousActiveClusterID := stateBeforeInvalidStatus.ActiveClusterID
	if err := s.c.reconcile(s.ctx, api.child); err != nil {
		t.Fatalf("reconcile with wrong target status should remain pending: %v", err)
	}
	if api.child.Status.Stage != "Restarted" || api.child.Status.Phase != "WaitingReady" {
		t.Fatalf("wrong recovery status ClusterID advanced the child: %+v", api.child.Status)
	}
	for _, peer := range api.child.Status.Peers {
		if peer.ClusterID != "wrong-generation-cluster" {
			t.Fatalf("fixture did not deliver the wrong ClusterID to target peer %q: %+v", peer.NodeID, peer)
		}
	}
	stateAfterInvalidStatus, _, err := s.c.loadAuto(s.ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	if stateAfterInvalidStatus.Intent == nil || stateAfterInvalidStatus.ActiveClusterID != previousActiveClusterID ||
		hashJSON(stateAfterInvalidStatus.Completed) != previousCompletionHash {
		t.Fatalf("wrong recovery status ClusterID was accepted or changed CONTROL: before=%+v after=%+v status=%+v",
			stateBeforeInvalidStatus, stateAfterInvalidStatus, s.r.Status)
	}
	s.api.mu.Lock()
	s.api.recoveryStatusClusterID = s.c.recoveryTargetID(api.child)
	s.api.mu.Unlock()
	for step := 0; step < 24 && api.child.Status.Stage != "Complete"; step++ {
		if err := s.c.reconcile(s.ctx, api.child); err != nil {
			t.Fatalf("reconcile generation child after restoring target status step %d (stage %q): %v", step, api.child.Status.Stage, err)
		}
	}
	if api.child.Status.Stage != "Complete" {
		t.Fatalf("production recovery reconcile did not complete the fenced generation: %+v", api.child.Status)
	}

	if err := s.c.reconcileCluster(s.ctx, s.r); err != nil {
		t.Fatalf("install verified generation completion: %v", err)
	}
	final, _, err := s.c.loadAuto(s.ctx, s.c.autoKey(s.r))
	if err != nil {
		t.Fatal(err)
	}
	if final.Intent != nil || final.Completed == nil || final.ActiveClusterID != api.child.Status.Target ||
		final.ActiveClusterID == previousClusterID ||
		len(final.Voters) != 3 || len(final.Identities) != 3 {
		t.Fatalf("completed generation was not installed: active=%q intent=%+v completed=%+v voters=%+v identities=%+v",
			final.ActiveClusterID, final.Intent, final.Completed, final.Voters, final.Identities)
	}
	if final.Completed.RecoveryUID != pinnedIntent.RecoveryUID ||
		final.Completed.RecoveryName != pinnedIntent.RecoveryName ||
		final.Completed.SpecHash != pinnedIntent.SpecHash ||
		final.Completed.FenceProofID != pinnedIntent.Proof.ProofID ||
		final.Completed.FenceRequestHash != pinnedIntent.Proof.RequestHash ||
		final.Completed.ClusterID != api.child.Status.Target {
		t.Fatalf("completion record is not bound to this fenced child and intent: completed=%+v intent=%+v childUID=%q target=%q",
			final.Completed, pinnedIntent, api.child.Metadata["uid"], api.child.Status.Target)
	}
	want := make(map[quepaxa.NodeID]FenceTarget, len(api.child.Status.TargetIdentities))
	for _, identity := range api.child.Status.TargetIdentities {
		nodeID := quepaxa.NodeID(identity.NodeID)
		if _, exists := want[nodeID]; exists {
			t.Fatalf("completed target contains duplicate voter identity %q: %+v", nodeID, api.child.Status.TargetIdentities)
		}
		want[nodeID] = identity
	}
	if len(want) != len(final.Voters) || len(want) != len(final.Identities) {
		t.Fatalf("completed target and installed voter/identity cardinalities differ: target=%d voters=%d identities=%d",
			len(want), len(final.Voters), len(final.Identities))
	}
	seenVoters := make(map[quepaxa.NodeID]struct{}, len(final.Voters))
	for _, voter := range final.Voters {
		if _, exists := seenVoters[voter.NodeID]; exists {
			t.Fatalf("installed voter inventory contains duplicate node %q: %+v", voter.NodeID, final.Voters)
		}
		seenVoters[voter.NodeID] = struct{}{}
		expected, exists := want[voter.NodeID]
		if !exists {
			t.Fatalf("installed voter %q is absent from completed target: %+v", voter.NodeID, api.child.Status.TargetIdentities)
		}
		identity, ok := final.Identities[voter.NodeID]
		if !ok || identity != expected {
			t.Fatalf("installed identity differs from the completed target: voter=%+v identity=%+v expected=%+v", voter, identity, expected)
		}
		delete(want, voter.NodeID)
	}
	for nodeID := range final.Identities {
		if _, exists := seenVoters[nodeID]; !exists {
			t.Fatalf("installed identity map contains non-voter node %q: %+v", nodeID, final.Identities)
		}
	}
	if len(want) != 0 || s.r.Status.Phase != "Stabilizing" {
		t.Fatalf("installed generation inventory is incomplete: remaining=%+v status=%+v", want, s.r.Status)
	}
}

// automaticGenerationRecoveryAPI supplies the normal Kubernetes resource and
// immutable Secret endpoints for a newly generated child, delegating all
// StatefulSet, Pod, Service, and other fixture behavior to operatorAPI.
type automaticGenerationRecoveryAPI struct {
	controller *Controller
	cluster    *ClusterResource
	peers      *operatorAPI
	child      *Resource
	secrets    map[string]object
}

func (a *automaticGenerationRecoveryAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	collection := a.controller.resourcePath("")
	secretCollection := a.controller.corePath("secrets", "")
	if r.URL.Path == collection && r.Method == http.MethodPost {
		var child Resource
		if err := json.NewDecoder(r.Body).Decode(&child); err != nil {
			http.Error(w, "invalid recovery", http.StatusBadRequest)
			return
		}
		child.Metadata["uid"] = "auto-generation-child-uid"
		child.Metadata["generation"] = float64(1)
		child.Metadata["resourceVersion"] = "1"
		a.child = &child
		a.peers.mu.Lock()
		a.peers.r = a.child
		a.peers.noQuorum = false
		ids := make([]string, 0, len(a.peers.voters))
		for _, member := range a.peers.voters {
			ids = append(ids, string(member.ID))
		}
		target := a.controller.recoveryTargetID(a.child)
		a.peers.recoveryStatusClusterID = target
		a.peers.membership = make(map[string]network.MembershipStatus, len(ids))
		for _, id := range ids {
			status := membershipStatus(id, 1, ids...)
			status.ClusterID = target
			status.WALIdentity = "generation-wal-" + id
			a.peers.membership[id] = status
		}
		a.peers.mu.Unlock()
		_ = json.NewEncoder(w).Encode(a.child)
		return
	}
	if strings.HasPrefix(r.URL.Path, collection+"/") {
		name := strings.TrimPrefix(r.URL.Path, collection+"/")
		isStatus := strings.HasSuffix(name, "/status")
		if isStatus {
			name = strings.TrimSuffix(name, "/status")
		}
		if r.Method == http.MethodGet {
			if a.child == nil || resourceName(a.child) != name {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(a.child)
			return
		}
		if r.Method == http.MethodPut && (r.URL.Path == collection+"/"+name || isStatus) {
			if a.child == nil || resourceName(a.child) != name {
				http.NotFound(w, r)
				return
			}
			var next Resource
			if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
				http.Error(w, "invalid recovery update", http.StatusBadRequest)
				return
			}
			*a.child = next
			a.peers.mu.Lock()
			a.peers.r = a.child
			a.peers.mu.Unlock()
			_ = json.NewEncoder(w).Encode(a.child)
			return
		}
	}
	if r.URL.Path == secretCollection && r.Method == http.MethodPost {
		var secret object
		if err := json.NewDecoder(r.Body).Decode(&secret); err != nil {
			http.Error(w, "invalid Secret", http.StatusBadRequest)
			return
		}
		meta := asObject(secret["metadata"])
		name := str(meta["name"])
		if name == "" {
			http.Error(w, "missing Secret name", http.StatusBadRequest)
			return
		}
		meta["uid"], meta["resourceVersion"] = "auto-generation-secret-uid", "1"
		a.secrets[name] = secret
		a.peers.mu.Lock()
		a.peers.secret = secret
		a.peers.mu.Unlock()
		_ = json.NewEncoder(w).Encode(secret)
		return
	}
	if strings.HasPrefix(r.URL.Path, secretCollection+"/") && r.Method == http.MethodGet {
		name := strings.TrimPrefix(r.URL.Path, secretCollection+"/")
		if secret := a.secrets[name]; secret != nil {
			_ = json.NewEncoder(w).Encode(secret)
			return
		}
		http.NotFound(w, r)
		return
	}
	if r.URL.Path == a.controller.clusterPath(str(a.cluster.Metadata["name"]))+"/status" && r.Method == http.MethodPut {
		var next ClusterResource
		if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
			http.Error(w, "invalid cluster status", http.StatusBadRequest)
			return
		}
		*a.cluster = next
		_ = json.NewEncoder(w).Encode(a.cluster)
		return
	}
	a.peers.serve(w, r)
}
