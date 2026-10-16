package operator

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// The child really completed before all target membership endpoints disappear.
// Losing the final health check must not make its source active again.
func TestAutomaticCompletionSurvivesTargetFailure(t *testing.T) {
	for _, mode := range []string{"target failure", "single target failure", "two target failures after adoption", "failure before identity capture", "failure before identity capture async", "failure before identity capture empty", "legacy completed child", "legacy completed child async", "capture unauthorized", "capture wrong cluster", "stale CAS", "bare complete", "changed reservation", "changed lineage", "duplicate quorum", "changed UID", "duplicate installed identity"} {
		t.Run(mode, func(t *testing.T) { testAutomaticCompletionOutcome(t, mode) })
	}
}

func TestAutomaticMembershipCompletionInstallsFailedLearner(t *testing.T) {
	ctx, child, api, c := membershipFixture(t)
	child.Spec.Container = "rhiza"
	child.Spec.Membership.OperationID = "recover"
	child.Spec.Membership.RemovedPod = "n3"
	api.learnerID = "learner"
	api.membership["learner"] = network.MembershipStatus{NodeID: "learner", ClusterID: "source", ConfigID: 1, Voters: []quepaxa.NodeID{"n1", "n2", "n3"}, WALIdentity: strings.Repeat("a", 64)}
	api.membershipSecret["data"].(object)["member"] = base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "learner", "new-token")))
	advance := func() {
		t.Helper()
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	advance() // remove journal
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	advance() // addition journal
	api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "learner")
	api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "learner")
	api.membership["learner"] = membershipStatus("learner", 3, "n1", "n2", "learner")
	status := api.membership["learner"]
	status.WALIdentity = strings.Repeat("a", 64)
	api.membership["learner"] = status
	advance()
	if child.Status.Phase != "Complete" {
		t.Fatalf("membership did not commit: %+v", child.Status)
	}
	r := autoClusterResource("cluster-1", "binding-uid-1", "logical-1")
	state := validAutoControl()
	state.StatefulSetUID = "sts-uid"
	state.Voters = []VoterPod{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}
	r.Spec.VoterPods = state.Voters
	request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID, SourceClusterID: "source", OperationID: "recover", Scope: ScopeVoter, Targets: []FenceTarget{{NodeID: "n3", Pod: "n3", PodUID: "pod-n3", WALIdentity: strings.Repeat("b", 64)}}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	state.Intent = &autoIntent{Request: request, RecoveryName: "recover", RecoveryUID: "resource-uid", Durability: "before-ack", Failed: "n3", Voters: append([]VoterPod(nil), state.Voters...), LearnerName: "learner", LearnerUID: "learner-uid", LearnerWAL: strings.Repeat("a", 64), Aborting: true, Proof: &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: hash, ProofID: child.Spec.Membership.Fence.Evidence, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}
	child.Spec.Membership.AbortAddition = true
	child.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}
	state.Intent.SpecHash = hashJSON(child.Spec)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Method == http.MethodGet && q.URL.Path == c.resourcePath("recover") {
			json.NewEncoder(w).Encode(child)
			return
		}
		if q.Method == http.MethodPut && q.URL.Path == c.clusterPath("cluster-1")+"/status" {
			json.NewEncoder(w).Encode(r)
			return
		}
		api.serve(w, q)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	fenced := 0
	c.Fencer = &countingFencer{count: &fenced}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	state, version, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	// Candidate and surviving voters fail after the committed completion.
	api.membership = nil
	if err := c.advanceAutomatic(ctx, r, state, version, api.sts); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Intent != nil || loaded.Completed == nil || len(loaded.Voters) != 3 || loaded.Voters[2].NodeID != "learner" || loaded.Identities["learner"].WALIdentity != strings.Repeat("a", 64) || loaded.Identities["learner"].PodUID != "learner-uid" {
		t.Fatalf("committed failed learner lost: %+v status=%+v", loaded, r.Status)
	}
}

func testAutomaticCompletionOutcome(t *testing.T, mode string) {
	legacy := strings.HasPrefix(mode, "legacy completed child")
	partial := strings.HasPrefix(mode, "failure before identity capture") || legacy
	twoFailures := mode == "two target failures after adoption"
	targetMode := "before-ack"
	if strings.HasSuffix(mode, " async") {
		targetMode = "async"
	}
	ctx, bucket, child, api, c := controllerFixture(t, "before-ack", targetMode)
	if targetMode == "async" {
		// The shared fixture's recovery HTTP responder is fixed to before-ack;
		// serve this candidate's actual configured target mode instead.
		c.HTTP = &http.Client{Transport: completionOutcomeTransport(func(q *http.Request) (*http.Response, error) {
			response, err := http.DefaultTransport.RoundTrip(q)
			if err != nil || q.URL.Path != "/recovery/status" {
				return response, err
			}
			defer response.Body.Close()
			var peer network.VoterRecoveryStatus
			if err := json.NewDecoder(response.Body).Decode(&peer); err != nil {
				return nil, err
			}
			peer.Durability = targetMode
			response.Body = io.NopCloser(bytes.NewReader(jsonBytes(peer)))
			return response, nil
		})}
	}
	child.Spec.RecoveryID = "recovery-1"
	child.Spec.Fence = fence(child)
	for range 12 {
		if err := c.reconcileAll(ctx); err != nil {
			t.Fatal(err)
		}
		if child.Status.Stage == "Restarted" {
			break
		}
	}
	child.Metadata["annotations"] = object{automaticOwner: "binding-uid-1", automaticLogical: "logical-1"}
	api.membership = map[string]network.MembershipStatus{}
	for _, id := range []string{"n1", "n2", "n3"} {
		if partial && (id == "n3" || strings.HasSuffix(mode, " empty")) {
			continue
		}
		status := membershipStatus(id, 1, "n1", "n2", "n3")
		status.ClusterID, status.WALIdentity = child.Status.Target, "target-wal-"+id
		api.membership[id] = status
	}
	if mode == "capture wrong cluster" {
		status := api.membership["n3"]
		status.ClusterID = "other"
		api.membership["n3"] = status
	}
	if mode == "capture unauthorized" {
		c.HTTP = &http.Client{Transport: completionOutcomeTransport(func(q *http.Request) (*http.Response, error) {
			if q.URL.Path == "/membership/status" && q.URL.Host == strings.TrimPrefix(api.peers[2].URL, "http://") {
				return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader("{}")), Header: make(http.Header)}, nil
			}
			return http.DefaultTransport.RoundTrip(q)
		})}
	}
	if err := c.startAndObserve(ctx, child, api.sts); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(mode, "capture ") {
		if child.Status.Phase != "WaitingReady" || child.Status.TargetIdentities != nil {
			t.Fatalf("untrusted identity was recorded as an unavailable target: %+v", child.Status)
		}
		return
	}
	if child.Status.Phase != "Complete" {
		t.Fatalf("child did not complete: %+v", child.Status)
	}
	r := autoClusterResource("cluster-1", "binding-uid-1", "logical-1")
	r.Spec.VoterPods = []VoterPod{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}
	state := validAutoControl()
	state.StatefulSetUID = "sts-uid"
	state.Voters = r.Spec.VoterPods
	request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID, SourceClusterID: "source", OperationID: "recovery-1", Scope: ScopeGeneration, Targets: []FenceTarget{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	state.Intent = &autoIntent{Request: request, RecoveryName: "recovery-1", RecoveryUID: "resource-uid", Durability: targetMode, Proof: &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID, RequestHash: hash, ProofID: "proof-1", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}
	child.Metadata["name"] = "recovery-1"
	child.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}
	child.Spec.Container = "rhiza"
	child.Spec.Fence.Evidence = state.Intent.Proof.ProofID
	child.Status.SpecHash = immutableSpecHash(child.Spec)
	state.Intent.SpecHash = hashJSON(child.Spec)
	// The reservation includes the immutable specification. Reproduce the
	// equivalent real reservation after normalizing the explicit container.
	if err := bucket.Upload(ctx, "root/source/recovery/successor.json", bytes.NewReader(jsonBytes(c.reservation(child)))); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Method == http.MethodGet && q.URL.Path == c.resourcePath("recovery-1") {
			json.NewEncoder(w).Encode(child)
			return
		}
		if q.Method == http.MethodPut && q.URL.Path == c.clusterPath("cluster-1")+"/status" {
			json.NewEncoder(w).Encode(r)
			return
		}
		api.serve(w, q)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	fenced := 0
	c.Fencer = &countingFencer{count: &fenced}
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	state, version, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	api.membership = nil
	switch mode {
	case "bare complete":
		child.Status = Status{Phase: "Complete", Stage: "Complete", Target: child.Status.Target}
	case "changed reservation":
		if err := bucket.Upload(ctx, "root/source/recovery/successor.json", bytes.NewReader([]byte(`{}`))); err != nil {
			t.Fatal(err)
		}
	case "changed lineage":
		if err := bucket.Upload(ctx, "root/"+child.Status.Target+"/recovery/generation.json", bytes.NewReader([]byte(`{}`))); err != nil {
			t.Fatal(err)
		}
	case "duplicate quorum":
		child.Status.Peers = append(child.Status.Peers[:1], child.Status.Peers[0])
	case "changed UID":
		child.Metadata["uid"] = "recreated-child"
	case "legacy completed child", "legacy completed child async":
		child.Status.TargetIdentities = nil
	case "duplicate installed identity":
		child.Status.TargetIdentities[2] = child.Status.TargetIdentities[0]
	case "stale CAS":
		if err := c.saveAuto(ctx, r, state, version); err != nil {
			t.Fatal(err)
		}
	}
	err = c.advanceAutomatic(ctx, r, state, version, api.sts)
	if mode == "stale CAS" {
		if !bucket.IsConditionNotMetErr(err) {
			t.Fatalf("stale completion did not fail CAS: %v", err)
		}
		state, version, err = c.loadAuto(ctx, c.autoKey(r))
		if err != nil || state.Intent == nil || state.ActiveClusterID != "source" {
			t.Fatalf("failed CAS advanced durable authority: %+v %v", state, err)
		}
		err = c.advanceAutomatic(ctx, r, state, version, api.sts)
	}
	if err != nil {
		t.Fatal(err)
	}
	loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if mode != "target failure" && mode != "single target failure" && !twoFailures && !partial && mode != "stale CAS" {
		if loaded.Intent == nil || loaded.ActiveClusterID != "source" || len(loaded.Completions) != 0 || r.Status.Phase != "Blocked" {
			t.Fatalf("invalid completion changed durable authority: %+v status=%+v", loaded, r.Status)
		}
		return
	}
	if loaded.Intent != nil || loaded.ActiveClusterID != child.Status.Target || len(loaded.Completions) != 1 {
		t.Fatalf("completed target lost after health failure: active=%s pending=%t completions=%d status=%+v", loaded.ActiveClusterID, loaded.Intent != nil, len(loaded.Completions), r.Status)
	}
	if len(loaded.Voters) != 3 || loaded.Voters[0].NodeID != quepaxa.NodeID("n1") || fenced != 0 {
		t.Fatal("installed inventory or fencing changed")
	}
	if loaded.Completed == nil || loaded.Completed.RecoveryUID != "resource-uid" || loaded.Completed.ClusterID != child.Status.Target {
		t.Fatal("installed outcome was not preserved independently of health")
	}
	wantKnown := 3
	if partial {
		wantKnown = 2
	}
	if legacy || strings.HasSuffix(mode, " empty") {
		wantKnown = 0
	}
	if len(loaded.Identities) != wantKnown || (!partial && (loaded.Identities["n3"].PodUID != "pod-n3" || loaded.Identities["n3"].WALIdentity != "target-wal-n3")) {
		t.Fatalf("installed target identities lost before next incident: %+v", loaded.Identities)
	}
	if mode == "single target failure" || twoFailures || partial {
		r.Spec.Automatic = true
		r.Spec.Policy = AutoPolicy{FailureGraceSeconds: 1, CooldownSeconds: 1, MaxRecoveriesPerDay: 10}
		loaded.BindingHash = autoBinding(r)
		loaded.SuspectedSince = time.Now().Add(-3 * time.Second)
		loaded.Completions = []time.Time{loaded.SuspectedSince}
		_, version, err = c.loadAuto(ctx, c.autoKey(r))
		if err != nil {
			t.Fatal(err)
		}
		if err := c.saveAuto(ctx, r, loaded, version); err != nil {
			t.Fatal(err)
		}
		api.membership = map[string]network.MembershipStatus{}
		for _, id := range []string{"n1", "n2"} {
			if twoFailures && id == "n2" {
				continue
			}
			status := membershipStatus(id, 1, "n1", "n2", "n3")
			status.ClusterID, status.WALIdentity = child.Status.Target, "target-wal-"+id
			api.membership[id] = status
		}
		ctr, err := container(api.sts, "rhiza")
		if err != nil {
			t.Fatal(err)
		}
		env, err := c.environment(ctx, ctr)
		if err != nil {
			t.Fatal(err)
		}
		c.HTTP = &http.Client{Transport: completionOutcomeTransport(func(q *http.Request) (*http.Response, error) {
			if q.URL.Path != "/recovery/probe" {
				return http.DefaultTransport.RoundTrip(q)
			}
			id := "n1"
			if q.URL.Host == strings.TrimPrefix(api.peers[1].URL, "http://") {
				id = "n2"
			}
			probe := network.RecoveryProbe{Excluded: q.URL.Query().Get("exclude"), Nonce: q.URL.Query().Get("nonce"), Status: network.VoterRecoveryStatus{NodeID: id, ClusterID: child.Status.Target, Quorum: !twoFailures}}
			mac := hmac.New(sha256.New, []byte(env["RHIZA_ADMIN_TOKEN"]))
			mac.Write(jsonBytes(probe))
			probe.MAC = hex.EncodeToString(mac.Sum(nil))
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(jsonBytes(probe))), Header: make(http.Header)}, nil
		})}
		if err := c.reconcileCluster(ctx, r); err != nil {
			t.Fatal(err)
		}
		next, _, err := c.loadAuto(ctx, c.autoKey(r))
		if err != nil {
			t.Fatal(err)
		}
		if partial && targetMode == "async" {
			if next.Intent != nil || next.ActiveClusterID != child.Status.Target || r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "async source requires allowDataLoss") || fenced != 0 {
				t.Fatalf("unknown incarnation bypassed async generation loss approval: %+v status=%+v", next, r.Status)
			}
			return
		}
		if partial || twoFailures {
			wantIdentities := 3
			if partial {
				wantIdentities = 2
			}
			if next.Intent == nil || next.Intent.Failed != "" || next.Intent.Request.Scope != ScopeGeneration || next.Intent.Request.SourceClusterID != child.Status.Target || next.Intent.RecoveryName == "recovery-1" || len(next.Intent.Request.Targets) != 3 || next.Intent.Proof != nil || fenced != 0 || len(next.Identities) != wantIdentities {
				t.Fatalf("unknown incarnation did not reserve fresh full-generation fencing: %+v status=%+v", next, r.Status)
			}
			reader, err := bucket.Get(ctx, "root/source/recovery/successor.json")
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(reader)
			reader.Close()
			if err != nil || !bytes.Equal(data, jsonBytes(c.reservation(child))) {
				t.Fatalf("old source reservation was reopened or reforked: %s %v", data, err)
			}
			return
		}
		if next.Intent == nil || next.Intent.Failed != "n3" || next.Intent.Request.SourceClusterID != child.Status.Target || next.Intent.RecoveryName == "recovery-1" || len(next.Intent.Request.Targets) != 1 || next.Intent.Request.Targets[0].WALIdentity != "target-wal-n3" || next.Intent.Request.Targets[0].PodUID != "pod-n3" {
			t.Fatalf("new incident cannot fence installed failed voter: %+v status=%+v", next, r.Status)
		}
		return
	}
	obs, _, err := c.observeAutomatic(ctx, r, loaded, api.sts)
	if err != nil || !obs.Known || obs.QuorumVerified || obs.Reachable != 0 {
		t.Fatalf("current target outage not retained by health observation: %+v %v", obs, err)
	}
	if ok, err := c.authorizeAutomaticChild(ctx, child); err != nil || ok {
		t.Fatalf("completed child retained execution authority: %t %v", ok, err)
	}
}

type completionOutcomeTransport func(*http.Request) (*http.Response, error)

func (f completionOutcomeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
