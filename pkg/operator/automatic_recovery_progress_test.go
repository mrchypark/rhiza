package operator

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func progressProbeResponse(req *http.Request, node string, live bool) *http.Response {
	p := network.RecoveryProbe{Nonce: req.URL.Query().Get("nonce"), Excluded: req.URL.Query().Get("exclude"), Status: network.VoterRecoveryStatus{NodeID: node, ClusterID: "source", Quorum: live}}
	data, _ := json.Marshal(p)
	mac := hmac.New(sha256.New, []byte("admin"))
	_, _ = mac.Write(data)
	p.MAC = hex.EncodeToString(mac.Sum(nil))
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(jsonBytes(p)))), Header: make(http.Header)}
}

func TestAutomaticRecoveryProgressEscalatesWithFullFencingInventory(t *testing.T) {
	for _, staleCAS := range []bool{false, true} {
		name := "reserved"
		if staleCAS {
			name = "CAS conflict"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			state := validAutoControl()
			r := autoClusterResource("cluster", state.BindingUID, "logical")
			c := autoController(autoBucket(t))
			seedArchive(t, ctx, c.Bucket, "root/source", members(), "before-ack")
			for _, ref := range state.Voters {
				state.Identities[ref.NodeID] = FenceTarget{NodeID: string(ref.NodeID), Pod: ref.Pod, PodUID: "uid-" + string(ref.NodeID), WALIdentity: "wal-" + string(ref.NodeID)}
			}
			request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID, SourceClusterID: "source", OperationID: "online-recovery", Scope: ScopeVoter, Targets: []FenceTarget{state.Identities["n1"]}}
			count := 0
			proof, err := (&countingFencer{count: &count}).Fence(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			child := Resource{Metadata: object{"name": "online-recovery", "uid": "old-child", "annotations": object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}}, Spec: Spec{StatefulSet: "rhiza", Container: "rhiza", SourceClusterID: "source", Membership: &MembershipSpec{OperationID: "online-recovery", Remove: "n1", ReplacementPod: "learner"}}}
			state.Intent = &autoIntent{Request: request, Proof: proof, Failed: "n1", Voters: append([]VoterPod(nil), state.Voters...), LearnerName: "learner", LearnerUID: "learner-uid", LearnerWAL: "learner-wal", RecoveryName: "online-recovery", RecoveryUID: "old-child", SpecHash: hashJSON(child.Spec), Durability: "before-ack", Policy: AutoPolicy{FailureGraceSeconds: 1}, NoQuorumSince: time.Now().Add(-time.Hour)}
			old := jsonBytes(state.Intent)
			token := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(token, []byte("kube"), 0600); err != nil {
				t.Fatal(err)
			}
			probes := 0
			client := &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
				if req.Method == http.MethodPut {
					data, err := io.ReadAll(req.Body)
					return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(data))), Header: make(http.Header)}, err
				}
				var body any
				if strings.Contains(req.URL.Path, "/pods/") {
					pod := path.Base(req.URL.Path)
					id := map[string]string{"rhiza-1": "n2", "rhiza-2": "n3", "learner": "learner"}[pod]
					if id == "" {
						return &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
					}
					uid := "uid-" + id
					if id == "learner" {
						uid = "learner-uid"
					}
					ip := map[string]string{"n2": "127.0.0.2", "n3": "127.0.0.3", "learner": "127.0.0.4"}[id]
					body = object{"metadata": object{"uid": uid}, "status": object{"podIP": ip}, "spec": object{"containers": []any{object{"name": "rhiza"}}}}
				} else if req.URL.Path == "/recovery/probe" {
					probes++
					if req.URL.Query().Get("exclude") != "n1" || req.Header.Get("Authorization") != "Bearer admin" {
						t.Error("active probe lost failed-voter exclusion or authentication")
					}
					id := map[string]string{"127.0.0.2": "n2", "127.0.0.3": "n3", "127.0.0.4": "learner"}[req.URL.Hostname()]
					return progressProbeResponse(req, id, false), nil
				} else {
					id := map[string]string{"127.0.0.2": "n2", "127.0.0.3": "n3", "127.0.0.4": "learner"}[req.URL.Hostname()]
					body = membershipStatus(id, 2, "n1", "n2", "n3")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(jsonBytes(body)))), Header: make(http.Header)}, nil
			})}
			c.HTTP = client
			c.Kube = &Kubernetes{BaseURL: "https://kube", Namespace: "default", TokenFile: token, Client: client}
			sts := object{"spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": []any{object{"name": "RHIZA_ADMIN_TOKEN", "value": "admin"}}}}}}}}
			if err := c.saveAuto(ctx, r, state, nil); err != nil {
				t.Fatal(err)
			}
			_, version, err := c.loadAuto(ctx, c.autoKey(r))
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := c.authorizeAutomaticChild(ctx, &child); err != nil || !ok {
				t.Fatalf("old child was not initially authorized: %t %v", ok, err)
			}
			if staleCAS {
				if err := c.saveAuto(ctx, r, state, version); err != nil {
					t.Fatal(err)
				}
			}
			if handled, err := c.maybeEscalateAutomatic(ctx, r, state, version, sts); !handled || staleCAS && !c.Bucket.IsConditionNotMetErr(err) || !staleCAS && err != nil {
				t.Fatalf("handled=%t stale=%t err=%v", handled, staleCAS, err)
			}
			loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
			if err != nil {
				t.Fatal(err)
			}
			if probes == 0 {
				t.Fatal("matching local snapshots bypassed active probes")
			}
			if staleCAS {
				if loaded.Intent.Request.Scope != ScopeVoter || loaded.Intent.RecoveryUID != "old-child" {
					t.Fatalf("losing CAS changed execution authority: %+v", loaded.Intent)
				}
				if ok, err := c.authorizeAutomaticChild(ctx, &child); err != nil || !ok {
					t.Fatalf("losing CAS revoked active child: %t %v", ok, err)
				}
				return
			}
			if loaded.Intent.Request.Scope != ScopeGeneration || loaded.Intent.Request.SourceClusterID != "source" || loaded.Intent.Request.OperationID == request.OperationID || len(loaded.Intent.Request.Targets) != 4 || loaded.Intent.Proof != nil {
				t.Fatalf("generation-wide fence was not freshly reserved: %+v", loaded.Intent)
			}
			want := map[string]FenceTarget{"learner": {NodeID: "learner", Pod: "learner", PodUID: "learner-uid", WALIdentity: "learner-wal"}}
			for id, target := range state.Identities {
				want[string(id)] = target
			}
			for _, target := range loaded.Intent.Request.Targets {
				if expected, ok := want[target.NodeID]; !ok || expected != target {
					t.Fatalf("incomplete or changed fence scope: %+v", target)
				}
				delete(want, target.NodeID)
			}
			if len(want) != 0 {
				t.Fatalf("missing fence targets: %+v", want)
			}
			reader, err := c.Bucket.Get(ctx, path.Join(c.Prefix, "automatic", c.Kube.Namespace, r.Spec.LogicalID, "superseded", loaded.Intent.Supersedes+".json"))
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if string(data) != string(old) {
				t.Fatal("superseded intent was not preserved exactly")
			}
			if ok, err := c.authorizeAutomaticChild(ctx, &child); err != nil || ok {
				t.Fatalf("superseded child retained authority: %t %v", ok, err)
			}
		})
	}
}

func TestAutomaticRecoveryProgressFencesInstalledGenerationAfterQuorumLoss(t *testing.T) {
	ctx, child, api, c := membershipFixture(t)
	child.Spec.Container = "rhiza"
	ctr, err := container(api.sts, "rhiza")
	if err != nil {
		t.Fatal(err)
	}
	ctr["env"] = replaceEnv(list(ctr["env"]), "RHIZA_ADMIN_TOKEN", object{"value": "admin"})
	child.Spec.Membership.OperationID = "recover"
	child.Spec.Membership.RemovedPod = "n3"
	seedLegacyRemoveMembershipJournal(child)
	api.learnerID = "learner"
	wal := strings.Repeat("a", 64)
	api.membership["learner"] = network.MembershipStatus{NodeID: "learner", ClusterID: "source", ConfigID: 1, Voters: []quepaxa.NodeID{"n1", "n2", "n3"}, WALIdentity: wal}
	api.membershipSecret["data"].(object)["member"] = base64.StdEncoding.EncodeToString(jsonBytes(voterMember("source", "learner", "new-token")))
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 2, "n1", "n2")
	api.membership["n2"] = membershipStatus("n2", 2, "n1", "n2")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if child.Status.Membership == nil || child.Status.Membership.Add == nil {
		n1, n2, learner := api.membership["n1"], api.membership["n2"], api.membership["learner"]
		t.Fatalf("committed learner fixture reached retirement without ADD journal: phase=%q message=%q membership=%+v config_n1=%d voters_n1=%v config_n2=%d voters_n2=%v learner={node:%s cluster:%s config:%d voting:%t pending:%t} changes=%d",
			child.Status.Phase, child.Status.Message, child.Status.Membership, n1.ConfigID, n1.Voters, n2.ConfigID, n2.Voters,
			learner.NodeID, learner.ClusterID, learner.ConfigID, learner.Voting, learner.Pending, len(api.changes))
	}
	if err := writeRetirementOperation(ctx, c, child); err != nil {
		t.Fatal(err)
	}
	api.membership["n1"] = membershipStatus("n1", 3, "n1", "n2", "learner")
	api.membership["n2"] = membershipStatus("n2", 3, "n1", "n2", "learner")
	for _, id := range []string{"n1", "n2"} {
		status := api.membership[id]
		status.WALIdentity = "wal-" + id
		api.membership[id] = status
	}
	api.replacement = nil
	delete(api.membership, "learner")
	child.Spec.Membership.AbortAddition = true
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if child.Status.Phase != "Complete" {
		t.Fatalf("unrecorded committed learner outcome was lost: %+v", child.Status)
	}
	r := autoClusterResource("cluster-1", "binding-uid-1", "logical-1")
	r.Spec.Automatic = true
	r.Spec.Policy = &AutoPolicy{FailureGraceSeconds: 1, CooldownSeconds: 1, MaxRecoveriesPerDay: 10}
	state := validAutoControl()
	state.StatefulSetUID = "sts-uid"
	state.Voters = []VoterPod{{NodeID: "n1", Pod: "n1"}, {NodeID: "n2", Pod: "n2"}, {NodeID: "n3", Pod: "n3"}}
	r.Spec.VoterCount = len(state.Voters)
	state.BindingHash = autoBinding(r)
	request := FenceRequest{LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default", StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID, SourceClusterID: "source", OperationID: "recover", Scope: ScopeVoter, Targets: []FenceTarget{{NodeID: "n3", Pod: "n3", PodUID: "pod-n3", WALIdentity: strings.Repeat("b", 64)}}}
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	state.Intent = &autoIntent{Request: request, RecoveryName: "recover", RecoveryUID: "resource-uid", Durability: "before-ack", Failed: "n3", Voters: append([]VoterPod(nil), state.Voters...), LearnerName: "learner", LearnerUID: "learner-uid", LearnerWAL: wal, Aborting: true, Proof: &FenceProof{OperationID: request.OperationID, SourceClusterID: "source", BindingUID: state.BindingUID, RequestHash: hash, ProofID: child.Spec.Membership.Fence.Evidence, ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}}
	child.Metadata["annotations"] = object{automaticOwner: state.BindingUID, automaticLogical: r.Spec.LogicalID}
	state.Intent.SpecHash = hashJSON(child.Spec)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
		if q.Method == http.MethodGet && q.URL.Path == "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters" {
			_ = json.NewEncoder(w).Encode(struct {
				Items []ClusterResource `json:"items"`
			}{Items: []ClusterResource{*r}})
			return
		}
		if q.Method == http.MethodGet && q.URL.Path == c.resourcePath("recover") {
			_ = json.NewEncoder(w).Encode(child)
			return
		}
		if q.Method == http.MethodPut && q.URL.Path == c.clusterPath("cluster-1")+"/status" {
			_ = json.NewEncoder(w).Encode(r)
			return
		}
		api.serve(w, q)
	}))
	t.Cleanup(server.Close)
	c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
	probeQuorum := true
	c.HTTP = &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path != "/recovery/probe" {
			return http.DefaultTransport.RoundTrip(req)
		}
		id := "n1"
		if req.URL.Host == strings.TrimPrefix(api.peers[1].URL, "http://") {
			id = "n2"
		}
		if req.Header.Get("Authorization") != "Bearer admin" {
			t.Error("missing probe authentication")
		}
		return progressProbeResponse(req, id, probeQuorum), nil
	})}
	fenced := 0
	var lastFence FenceRequest
	c.Fencer = retirementFencer(func(ctx context.Context, request FenceRequest) (*FenceProof, error) {
		lastFence = request
		return (&countingFencer{count: &fenced}).Fence(ctx, request)
	})
	if err := c.saveAuto(ctx, r, state, nil); err != nil {
		t.Fatal(err)
	}
	state, version, err := c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.advanceAutomatic(ctx, r, state, version, api.sts); err != nil {
		t.Fatal(err)
	}
	state, version, err = c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent != nil || state.Completed == nil || state.Identities["learner"].PodUID != "learner-uid" {
		t.Fatalf("failed committed voter was not installed: %+v", state)
	}
	// Advance the persisted grace/cooldown clocks without timing-dependent sleeps.
	state.SuspectedSince = time.Now().Add(-time.Hour)
	state.Completions[0] = time.Now().Add(-time.Hour)
	if err := c.saveAuto(ctx, r, state, version); err != nil {
		t.Fatal(err)
	}
	// A verified quorum keeps the controller in Degraded; model the subsequent
	// voter failure that is supposed to trigger the next recovery operation.
	probeQuorum = false
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	state, _, err = c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if state.Intent == nil || state.Intent.Request.Scope != ScopeGeneration || state.Intent.Request.OperationID == "recover" {
		t.Fatalf("quorum loss did not reserve a fresh generation recovery: %+v status=%+v", state.Intent, r.Status)
	}
	wantTargets := map[string]FenceTarget{}
	for _, voter := range state.Voters {
		target, ok := state.Identities[voter.NodeID]
		if !ok {
			t.Fatalf("installed voter %q has no captured identity: %+v", voter.NodeID, state.Identities)
		}
		target.NodeID = string(voter.NodeID)
		wantTargets[string(voter.NodeID)] = target
	}
	if len(wantTargets) != 3 || wantTargets["n1"].NodeID != "n1" || wantTargets["n2"].NodeID != "n2" ||
		wantTargets["learner"].NodeID != "learner" || wantTargets["n3"].NodeID != "" || len(state.Intent.Request.Targets) != len(wantTargets) {
		t.Fatalf("generation recovery did not use the installed voter set without retired n3: voters=%v identities=%+v targets=%+v", state.Voters, state.Identities, state.Intent.Request.Targets)
	}
	for _, target := range state.Intent.Request.Targets {
		if expected, ok := wantTargets[target.NodeID]; !ok || expected != target {
			t.Fatalf("generation recovery target is not the captured installed identity: got=%+v want=%+v", target, expected)
		}
		delete(wantTargets, target.NodeID)
	}
	if len(wantTargets) != 0 {
		t.Fatalf("generation recovery omitted installed voter identities: %+v", wantTargets)
	}
	if err := c.reconcileCluster(ctx, r); err != nil {
		t.Fatal(err)
	}
	state, version, err = c.loadAuto(ctx, c.autoKey(r))
	if err != nil {
		t.Fatal(err)
	}
	if fenced != 1 || state.Intent.Proof == nil || len(lastFence.Targets) != 3 {
		t.Fatalf("next replacement was not fenced: count=%d intent=%+v", fenced, state.Intent)
	}
	fencedTargets := make(map[string]bool, len(lastFence.Targets))
	for _, target := range lastFence.Targets {
		fencedTargets[target.NodeID] = true
	}
	if !fencedTargets["n1"] || !fencedTargets["n2"] || !fencedTargets["learner"] || fencedTargets["n3"] {
		t.Fatalf("generation fence did not preserve exact installed voter scope: %+v", lastFence.Targets)
	}
}

// The Kubernetes fencing backend admits a fixed number of targets, so
// automatic recovery must never reserve a scope the outage cannot execute.
func TestAutomaticReservationRejectsUnfenceableScope(t *testing.T) {
	for _, tc := range []struct {
		name     string
		voters   int
		learners int
		blocked  bool
		want     string
	}{
		{name: "voters at capacity", voters: 64},
		{name: "voters plus history at capacity", voters: 63, learners: 1},
		{name: "voter count over capacity", voters: 65, blocked: true, want: "spec.voterCount=65 exceeds the maximum 64"},
		{name: "voters plus history over capacity", voters: 64, learners: 1, blocked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			voters := clusterVoterMembers(tc.voters)
			_, _, _, api, c := controllerFixtureVoters(t, "before-ack", "before-ack", voters)
			ctr, err := container(api.sts, "rhiza")
			if err != nil {
				t.Fatal(err)
			}
			ctr["env"] = append(list(ctr["env"]), object{"name": "RHIZA_ADMIN_TOKEN", "value": "admin"})
			r := autoClusterResource("cluster-1", "binding-uid-1", "logical-1")
			r.Spec.Container, r.Spec.Automatic = "rhiza", true
			r.Spec.Policy = &AutoPolicy{FailureGraceSeconds: 1, CooldownSeconds: 1, MaxRecoveriesPerDay: 10}
			voterPods := make([]VoterPod, 0, tc.voters)
			for _, member := range voters {
				voterPods = append(voterPods, VoterPod{NodeID: member.ID, Pod: string(member.ID)})
			}
			r.Spec.VoterCount = tc.voters
			state := validAutoControl()
			state.BindingUID, state.StatefulSetUID = "binding-uid-1", "sts-uid"
			state.BindingHash = autoBinding(r)
			state.Voters, state.SuspectedSince = voterPods, time.Now().Add(-time.Hour)
			for _, member := range voters {
				state.Identities[member.ID] = FenceTarget{NodeID: string(member.ID), Pod: string(member.ID), PodUID: "pod-" + string(member.ID), WALIdentity: "wal-" + string(member.ID)}
			}
			for i := range tc.learners {
				pod := "learner-" + strconv.Itoa(i)
				state.Learners = append(state.Learners, learnerIncarnation{SourceClusterID: "source", OperationID: "retire-1", CredentialUID: "cred-1", Target: FenceTarget{NodeID: pod, Pod: pod, PodUID: "pod-" + pod, WALIdentity: "wal-" + pod}})
			}
			// These target identities are intentionally absent from the fake API;
			// keep their cleanup unresolved so this test measures reserved scope
			// capacity rather than accidentally compacting it first.
			c.Fencer = retirementFencer(func(context.Context, FenceRequest) (*FenceProof, error) {
				return nil, ErrFencePending
			})
			api.membership = nil
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
				if q.Method == http.MethodPut && q.URL.Path == c.clusterPath("cluster-1")+"/status" {
					json.NewEncoder(w).Encode(r)
					return
				}
				api.serve(w, q)
			}))
			t.Cleanup(server.Close)
			c.Kube.BaseURL, c.Kube.Client = server.URL, server.Client()
			if err := c.saveAuto(ctx, r, state, nil); err != nil {
				t.Fatal(err)
			}
			if err := c.reconcileCluster(ctx, r); err != nil {
				t.Fatal(err)
			}
			loaded, _, err := c.loadAuto(ctx, c.autoKey(r))
			if err != nil {
				t.Fatal(err)
			}
			if !tc.blocked {
				if loaded.Intent == nil || loaded.Intent.Request.Scope != ScopeGeneration || len(loaded.Intent.Request.Targets) != fenceTargetCapacity || r.Status.Phase != string(AutoRecover) {
					t.Fatalf("capacity boundary did not reserve fencing: %+v status=%+v", loaded.Intent, r.Status)
				}
				return
			}
			want := tc.want
			if want == "" {
				want = strconv.Itoa(tc.voters+tc.learners) + " targets exceed the fence capacity of " + strconv.Itoa(fenceTargetCapacity)
			}
			if loaded.Intent != nil || r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, want) {
				t.Fatalf("unfenceable scope was reserved: %+v status=%+v want=%q", loaded.Intent, r.Status, want)
			}
		})
	}
}
