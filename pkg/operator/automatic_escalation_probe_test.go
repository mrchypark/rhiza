package operator

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type escalationProbeTransport func(*http.Request) (*http.Response, error)

func (f escalationProbeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func signVersionedRecoveryProbe(p *network.RecoveryProbe, token string) {
	p.Version = network.RecoveryProbeProtocolVersion
	p.MAC = ""
	data, _ := json.Marshal(p)
	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write(data)
	p.MAC = hex.EncodeToString(mac.Sum(nil))
}

type deadlineReleasedProbeBody struct {
	ctx  context.Context
	data []byte
	done bool
}

func (b *deadlineReleasedProbeBody) Read(p []byte) (int, error) {
	if b.done {
		return 0, io.EOF
	}
	<-b.ctx.Done()
	b.done = true
	return copy(p, b.data), nil
}

func (b *deadlineReleasedProbeBody) Close() error { return nil }

func TestAutomaticQuorumProbeDoesNotAcceptBodyReleasedAtCallDeadline(t *testing.T) {
	client := &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
		status := network.VoterRecoveryStatus{NodeID: "n2", ClusterID: "source", ConfigID: 2,
			Voters: []string{"n1", "n2", "n3"}, WALIdentity: "wal-n2", Voting: true,
			Quorum: true, QuorumKnown: true, QuorumConfigID: 2, ReadIndexSlot: 8}
		probe := network.RecoveryProbe{Nonce: req.URL.Query().Get("nonce"), Status: status}
		signVersionedRecoveryProbe(&probe, "admin")
		return &http.Response{StatusCode: http.StatusOK,
			Body: &deadlineReleasedProbeBody{ctx: req.Context(), data: jsonBytes(probe)}, Header: make(http.Header)}, nil
	})}
	c := &Controller{HTTP: client}
	outer, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, got, err := c.automaticQuorumProbeWithTimeout(outer, "http://127.0.0.1/recovery/probe", "admin", "source", "n2", 20*time.Millisecond)
	if err != nil || got != quorumProbeNoResponse || status.WALIdentity != "" {
		t.Fatalf("late body crossed call deadline: status=%+v result=%v err=%v", status, got, err)
	}
	if outer.Err() != nil {
		t.Fatalf("test did not preserve live outer context: %v", outer.Err())
	}
}

func TestAutomaticSurvivingQuorumBindsCapturedIncarnation(t *testing.T) {
	for _, tc := range []struct {
		name       string
		badWAL     bool
		podUIDSwap bool
		podIPSwap  bool
		badMembers bool
	}{
		{name: "captured source identity and membership"},
		{name: "signed source WAL differs from fencing inventory", badWAL: true},
		{name: "source Pod UID changes during probe", podUIDSwap: true},
		{name: "source Pod IP changes during probe", podIPSwap: true},
		{name: "observed membership escapes active inventory", badMembers: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := validAutoControl()
			for _, ref := range state.Voters {
				state.Identities[ref.NodeID] = FenceTarget{NodeID: string(ref.NodeID), Pod: ref.Pod, PodUID: "uid-" + string(ref.NodeID), WALIdentity: "wal-" + string(ref.NodeID)}
			}
			r := autoClusterResource("cluster", "uid", "logical")
			c := autoController(autoBucket(t))
			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("kube"), 0600); err != nil {
				t.Fatal(err)
			}
			podGets := map[string]int{}
			client := &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
				if strings.HasPrefix(req.URL.Path, "/api/v1/namespaces/default/pods/") {
					name := filepath.Base(req.URL.Path)
					id := map[string]string{"rhiza-1": "n2", "rhiza-2": "n3"}[name]
					if id == "" {
						return &http.Response{StatusCode: http.StatusNotFound, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
					}
					podGets[name]++
					uid := "uid-" + id
					ip := map[string]string{"n2": "127.0.0.2", "n3": "127.0.0.3"}[id]
					if name == "rhiza-1" && tc.podUIDSwap && podGets[name] > 1 {
						uid = "replacement-uid"
					}
					if name == "rhiza-1" && tc.podIPSwap && podGets[name] > 1 {
						ip = "127.0.0.99"
					}
					body := object{"metadata": object{"name": name, "uid": uid}, "status": object{"podIP": ip}, "spec": object{"containers": []any{object{"name": "rhiza", "ports": []any{object{"name": "http", "containerPort": 8080}}}}}}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(jsonBytes(body)))), Header: make(http.Header)}, nil
				}
				id := map[string]string{"127.0.0.2": "n2", "127.0.0.3": "n3"}[req.URL.Hostname()]
				if req.URL.Path != "/recovery/probe" || id == "" || req.Header.Get("Authorization") != "Bearer admin" {
					t.Fatalf("unexpected request %s", req.URL)
				}
				voters := []string{"n1", "n2", "n3"}
				if tc.badMembers {
					voters = []string{"n1", "n2", "other"}
				}
				wal := "wal-" + id
				if tc.badWAL && id == "n2" {
					wal = "replacement-wal"
				}
				status := network.VoterRecoveryStatus{NodeID: id, ClusterID: "source", ConfigID: 2, Voters: voters, WALIdentity: wal, Voting: true, Quorum: true, QuorumKnown: true, QuorumConfigID: 2, ReadIndexSlot: 8}
				probe := network.RecoveryProbe{Nonce: req.URL.Query().Get("nonce"), Excluded: "n1", Status: status}
				signVersionedRecoveryProbe(&probe, "admin")
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(jsonBytes(probe)))), Header: make(http.Header)}, nil
			})}
			c.HTTP = client
			c.Kube = &Kubernetes{BaseURL: "https://kube", Namespace: "default", TokenFile: tokenFile, Client: client}
			sts := object{"spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": []any{object{"name": "RHIZA_ADMIN_TOKEN", "value": "admin"}}}}}}}}
			got, err := c.automaticSurvivingQuorum(context.Background(), r, state, sts, "n1")
			if tc.badWAL || tc.podUIDSwap || tc.podIPSwap || tc.badMembers {
				if err == nil || got == quorumProbeAvailable {
					t.Fatalf("unsafe positive accepted: state=%v err=%v", got, err)
				}
				return
			}
			if err != nil || got != quorumProbeAvailable {
				t.Fatalf("captured live quorum rejected: state=%v err=%v", got, err)
			}
		})
	}
}

func TestAutomaticEscalationRequiresLiveQuorum(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		live, learnerOnly       bool
		corrupt                 string
		zeroSlot                bool
		unknownRead             bool
		noTimer                 bool
		expired                 bool
		reserve                 bool
		outerCanceled           bool
		mixed, unknown, unbound bool
		mixedMembership         bool
		unapprovedMembership    bool
		prospectiveLearner      bool
		uidWithoutWAL           bool
		recreated               bool
		sourceWALMismatch       bool
		sourcePodReplaced       bool
		sourceIPChanged         bool
	}{
		{name: "matching isolated snapshots"},
		{name: "active surviving quorum", live: true},
		{name: "committed replacement only reachable", live: true, learnerOnly: true},
		{name: "replayed challenge", live: true, corrupt: "nonce"},
		{name: "invalid signature", live: true, corrupt: "mac"},
		{name: "wrong identity", live: true, corrupt: "identity"},
		{name: "wrong exclusion", live: true, corrupt: "exclude"},
		{name: "wrong cluster", live: true, corrupt: "cluster"},
		{name: "expired grace still requires archive", expired: true},
		{name: "mixed known configurations with live quorum", mixed: true, live: true},
		{name: "mixed known configurations without live quorum", mixed: true},
		{name: "approved pre-add and post-add snapshots advance no-quorum fence intent", mixedMembership: true, expired: true, reserve: true},
		{name: "known but unapproved membership blocks timer and reservation", unapprovedMembership: true, expired: true},
		{name: "prospective learner without captured identity does not block quorum progress", prospectiveLearner: true, live: true, noTimer: true},
		{name: "UID-known learner without WAL is not treated as probe identity", uidWithoutWAL: true, live: true},
		{name: "unknown membership must block live proof", mixed: true, unknown: true, live: true},
		{name: "matching unknown membership must block live proof", unknown: true, live: true},
		{name: "versioned unknown is absence of fresh positive", corrupt: "unknown", noTimer: true},
		{name: "per-probe timeout starts policy period", corrupt: "timeout", noTimer: true},
		{name: "all-timeout expired grace still requires archive", corrupt: "timeout", expired: true},
		{name: "all-timeout grace reserves fence intent without child", corrupt: "timeout", expired: true, reserve: true},
		{name: "stable negative plus unknown peer counts policy period", mixed: true, unknownRead: true, expired: true},
		{name: "legacy payload blocks policy period", corrupt: "legacy", expired: true},
		{name: "contradictory quorum flags block policy period", corrupt: "both-quorum-flags", expired: true},
		{name: "pending unavailable payload is invalid", corrupt: "unavailable-pending", expired: true},
		{name: "unavailable result with stale read index is invalid", corrupt: "unavailable-slot", expired: true},
		{name: "unavailable result with quorum config is invalid", corrupt: "unavailable-config", expired: true},
		{name: "authentication failure blocks policy period", corrupt: "auth", expired: true},
		{name: "invalid observation HTTP status blocks policy period", corrupt: "invalid-status", expired: true},
		{name: "stable authenticated no quorum starts timer", noTimer: true},
		{name: "successful zero-slot ReadIndex is positive", live: true, zeroSlot: true},
		{name: "outer cancellation does not advance policy timer", outerCanceled: true},
		{name: "unknown membership must block escalation", mixed: true, unknown: true, expired: true},
		{name: "unbound learner must block escalation", mixed: true, unbound: true, expired: true},
		{name: "recreated learner must block escalation", mixed: true, recreated: true, expired: true},
		{name: "source WAL mismatch blocks without timer progress", sourceWALMismatch: true, expired: true},
		{name: "source Pod replacement after probe blocks without timer progress", sourcePodReplaced: true, expired: true},
		{name: "source Pod IP change after probe blocks without timer progress", sourceIPChanged: true, expired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := validAutoControl()
			for _, ref := range state.Voters {
				state.Identities[ref.NodeID] = FenceTarget{NodeID: string(ref.NodeID), Pod: ref.Pod, PodUID: "uid-" + string(ref.NodeID), WALIdentity: "wal-" + string(ref.NodeID)}
			}
			since := time.Now().UTC().Add(-time.Second)
			if tc.expired {
				since = since.Add(-time.Hour)
			}
			noQuorumSince := since
			if tc.noTimer {
				noQuorumSince = time.Time{}
			}
			includeLearner := tc.learnerOnly || tc.unbound || tc.recreated || tc.mixedMembership || tc.unapprovedMembership || tc.prospectiveLearner || tc.uidWithoutWAL
			learnerName := ""
			if includeLearner {
				learnerName = "learner"
			}
			state.Intent = &autoIntent{Failed: "n1", Voters: state.Voters, LearnerName: learnerName, Policy: AutoPolicy{FailureGraceSeconds: 600}, NoQuorumSince: noQuorumSince}
			if includeLearner && !tc.unbound && !tc.prospectiveLearner && !tc.uidWithoutWAL {
				state.Intent.LearnerUID, state.Intent.LearnerWAL = "learner-uid", "learner-wal"
			} else if tc.uidWithoutWAL {
				state.Intent.LearnerUID = "learner-uid"
			}
			if tc.prospectiveLearner {
				request := FenceRequest{
					LogicalID: "logical", BindingUID: state.BindingUID, Namespace: "default",
					StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID,
					SourceClusterID: state.ActiveClusterID, OperationID: "auto-prospective-learner",
					Scope: ScopeVoter, Targets: []FenceTarget{state.Identities["n1"]},
				}
				hash, err := requestHash(&request)
				if err != nil {
					t.Fatal(err)
				}
				state.Intent.RecoveryName = request.OperationID
				state.Intent.Request = request
				state.Intent.Proof = &FenceProof{OperationID: request.OperationID, SourceClusterID: state.ActiveClusterID, BindingUID: state.BindingUID, RequestHash: hash, ProofID: "verified-voter-proof", ProcessesTerminated: true, RecreationBlocked: true, StorageQuiesced: true}
			}
			r := autoClusterResource("cluster", "uid", "logical")
			c := autoController(autoBucket(t))
			if tc.reserve {
				seedArchive(t, context.Background(), c.Bucket, "root/source", members(), "before-ack")
				state.Intent.Durability = "before-ack"
				state.Intent.Request = FenceRequest{
					LogicalID: r.Spec.LogicalID, BindingUID: state.BindingUID, Namespace: "default",
					StatefulSet: "rhiza", StatefulSetUID: state.StatefulSetUID,
					SourceClusterID: state.ActiveClusterID, OperationID: "online-recovery",
					Scope: ScopeVoter, Targets: []FenceTarget{state.Identities["n1"]},
				}
			}
			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("kube"), 0600); err != nil {
				t.Fatal(err)
			}
			probes := 0
			childPosts := 0
			podGets := map[string]int{}
			client := &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
				code := 200
				var body any
				if req.Method == http.MethodPost {
					childPosts++
					return &http.Response{StatusCode: http.StatusCreated, Body: io.NopCloser(strings.NewReader(`{"metadata":{"uid":"unexpected"}}`)), Header: make(http.Header)}, nil
				}
				if req.Method == http.MethodPut {
					data, err := io.ReadAll(req.Body)
					if err != nil {
						return nil, err
					}
					return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(string(data))), Header: http.Header{}}, nil
				}
				if strings.Contains(req.URL.Path, "/pods/") {
					pod := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
					podGets[pod]++
					id := map[string]string{"rhiza-1": "n2", "rhiza-2": "n3", "learner": "learner"}[pod]
					ip := map[string]string{"rhiza-1": "127.0.0.2", "rhiza-2": "127.0.0.3", "learner": "127.0.0.4"}[pod]
					if ip == "" {
						code = 404
					}
					uid := "uid-" + id
					if id == "learner" {
						uid = "learner-uid"
					}
					if tc.recreated && pod == "learner" {
						uid = "different-uid"
					}
					if tc.sourcePodReplaced && pod == "rhiza-1" && podGets[pod] > 1 {
						uid = "replacement-uid"
					}
					if tc.sourceIPChanged && pod == "rhiza-1" && podGets[pod] > 1 {
						ip = "127.0.0.99"
					}
					body = object{"metadata": object{"uid": uid}, "status": object{"podIP": ip}, "spec": object{"containers": []any{object{"name": "rhiza"}}}}
				} else {
					id := map[string]string{"127.0.0.2": "n2", "127.0.0.3": "n3", "127.0.0.4": "learner"}[req.URL.Hostname()]
					if req.Header.Get("Authorization") != "Bearer admin" {
						t.Error("missing authentication")
					}
					if tc.learnerOnly && id != "learner" {
						code = 503
					}
					if req.URL.Path == "/recovery/probe" {
						probes++
						if req.URL.Query().Get("version") != "1" {
							t.Error("recovery probe did not request protocol version 1")
						}
						if tc.unknownRead && id == "n3" {
							return nil, context.DeadlineExceeded
						}
						if tc.corrupt == "timeout" {
							return nil, context.DeadlineExceeded
						}
						if tc.corrupt == "auth" {
							return &http.Response{StatusCode: http.StatusForbidden, Body: io.NopCloser(strings.NewReader(`{"code":"forbidden","error":"forbidden"}`)), Header: make(http.Header)}, nil
						}
						if tc.corrupt == "invalid-status" {
							return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader(`{"code":"invalid_recovery_observation","error":"invalid"}`)), Header: make(http.Header)}, nil
						}
						voters := []quepaxa.NodeID{"n1", "n2", "n3"}
						config := uint(2)
						if tc.learnerOnly || includeLearner && id == "learner" {
							voters = []quepaxa.NodeID{"n2", "n3", "learner"}
							config = 3
						} else if tc.mixed && id != "n2" {
							config = 3
						}
						if tc.mixedMembership && id == "n3" {
							voters = []quepaxa.NodeID{"n2", "n3", "learner"}
							config = 3
						}
						if tc.unapprovedMembership && id != "n2" {
							voters = []quepaxa.NodeID{"n1", "n3", "learner"}
							config = 3
						}
						if tc.unbound && id == "n3" {
							voters = []quepaxa.NodeID{"n2", "n3", "learner"}
							config = 3
						}
						if tc.unknown {
							voters = append(voters, "unknown-voter")
						}
						voterStrings := make([]string, len(voters))
						for i, voter := range voters {
							voterStrings[i] = string(voter)
						}
						walIdentity := "wal-" + id
						if tc.sourceWALMismatch && id == "n2" {
							walIdentity = "replacement-wal"
						}
						if id == "learner" {
							walIdentity = "learner-wal"
						}
						status := network.VoterRecoveryStatus{NodeID: id, ClusterID: "source", ConfigID: config, Voters: voterStrings, WALIdentity: walIdentity, Voting: true, Quorum: tc.live, QuorumKnown: tc.live, QuorumUnavailable: !tc.live}
						if tc.live {
							status.QuorumConfigID = config
							if !tc.zeroSlot {
								status.ReadIndexSlot = 19
							}
						}
						if tc.corrupt == "unknown" {
							status.Quorum = false
							status.QuorumKnown = false
							status.QuorumUnavailable = false
							status.QuorumConfigID = 0
							status.Pending = true
						}
						if tc.corrupt == "both-quorum-flags" {
							status.QuorumKnown = true
							status.QuorumUnavailable = true
							status.Quorum = false
							status.QuorumConfigID = config
							status.ReadIndexSlot = 0
						}
						if tc.corrupt == "unavailable-slot" {
							status.ReadIndexSlot = 19
						}
						if tc.corrupt == "unavailable-config" {
							status.QuorumConfigID++
						}
						if tc.corrupt == "unavailable-pending" {
							status.Pending = true
						}
						if tc.corrupt == "legacy" {
							status.ConfigID = 0
							status.Voters = nil
							status.WALIdentity = ""
							status.Voting = false
							status.QuorumKnown = false
						}
						p := network.RecoveryProbe{Nonce: req.URL.Query().Get("nonce"), Excluded: req.URL.Query().Get("exclude"), Status: status}
						if p.Excluded != "n1" {
							t.Error("failed voter was not excluded")
						}
						if tc.corrupt == "nonce" {
							p.Nonce = "old"
						}
						if tc.corrupt == "identity" {
							p.Status.NodeID = "different"
						}
						if tc.corrupt == "exclude" {
							p.Excluded = "different"
						}
						if tc.corrupt == "cluster" {
							p.Status.ClusterID = "different"
						}
						signVersionedRecoveryProbe(&p, "admin")
						if tc.corrupt == "legacy" {
							p.Version = 0
						}
						if tc.corrupt == "mac" {
							p.MAC = "00"
						}
						body = p
					} else {
						t.Fatalf("unexpected endpoint %s", req.URL)
					}
				}
				data, _ := json.Marshal(body)
				return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(string(data))), Header: http.Header{}}, nil
			})}
			c.HTTP = client
			c.Kube = &Kubernetes{BaseURL: "https://kube", Namespace: "default", TokenFile: tokenFile, Client: client}
			sts := object{"spec": object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": []any{object{"name": "RHIZA_ADMIN_TOKEN", "value": "admin"}}}}}}}}
			if err := c.saveAuto(context.Background(), r, state, nil); err != nil {
				t.Fatal(err)
			}
			_, version, err := c.loadAuto(context.Background(), c.autoKey(r))
			if err != nil {
				t.Fatal(err)
			}
			probeCtx := context.Background()
			if tc.outerCanceled {
				canceledCtx, cancel := context.WithCancel(probeCtx)
				cancel()
				probeCtx = canceledCtx
			}
			handled, err := c.maybeEscalateAutomatic(probeCtx, r, state, version, sts)
			if tc.prospectiveLearner && (handled || err != nil || probes != 2 || podGets["learner"] != 0) {
				t.Fatalf("unprovisioned learner blocked the ordinary voter-fenced progression: handled=%v err=%v probes=%d podGets=%v status=%+v", handled, err, probes, podGets, r.Status)
			}
			if tc.outerCanceled {
				if err == nil || probes != 0 || !state.Intent.NoQuorumSince.Equal(since) {
					t.Fatalf("outer-canceled reconcile advanced timer: err=%v probes=%d since=%v", err, probes, state.Intent.NoQuorumSince)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if tc.reserve {
				loaded, savedVersion, err := c.loadAuto(context.Background(), c.autoKey(r))
				if err != nil {
					t.Fatal(err)
				}
				wantTargets := len(state.Voters)
				if tc.mixedMembership {
					wantTargets++
				}
				if loaded.Intent == nil || loaded.Intent.Proof != nil || loaded.Intent.Request.Scope != ScopeGeneration || len(loaded.Intent.Request.Targets) != wantTargets {
					t.Fatalf("all-timeout round did not durably reserve generation fencing: %+v", loaded.Intent)
				}
				fenceCalls := 0
				c.Fencer = retirementFencer(func(_ context.Context, request FenceRequest) (*FenceProof, error) {
					fenceCalls++
					if request.Scope != ScopeGeneration {
						t.Errorf("fence scope=%s, want generation", request.Scope)
					}
					return nil, ErrFencePending
				})
				if err := c.advanceAutomatic(context.Background(), r, loaded, savedVersion, sts); err != nil {
					t.Fatal(err)
				}
				if fenceCalls != 1 || childPosts != 0 || loaded.Intent.Proof != nil {
					t.Fatalf("intent bypassed pending independent proof: fenceCalls=%d childPosts=%d intent=%+v", fenceCalls, childPosts, loaded.Intent)
				}
				return
			}
			identityMismatch := tc.sourceWALMismatch || tc.sourcePodReplaced || tc.sourceIPChanged
			wantLive := tc.live && tc.corrupt == "" && !tc.unknown && !tc.unbound && !tc.recreated && !identityMismatch
			invalidProbe := tc.corrupt != "" && tc.corrupt != "timeout" && tc.corrupt != "unknown"
			if tc.unapprovedMembership {
				loaded, _, err := c.loadAuto(context.Background(), c.autoKey(r))
				if err != nil {
					t.Fatal(err)
				}
				if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "observed membership differs from the active intent") || !state.Intent.NoQuorumSince.Equal(since) || loaded.Intent == nil || loaded.Intent.Request.Scope == ScopeGeneration || loaded.Intent.Supersedes != "" || childPosts != 0 {
					t.Fatalf("known but unapproved membership advanced recovery: status=%+v intent=%+v childPosts=%d", r.Status, loaded.Intent, childPosts)
				}
				return
			}
			if tc.prospectiveLearner || tc.uidWithoutWAL {
				if r.Status.Phase == "Blocked" || podGets["learner"] != 0 {
					t.Fatalf("uncaptured learner blocked or entered voter probing: status=%+v podGets=%v", r.Status, podGets)
				}
				if tc.uidWithoutWAL && (state.Intent.LearnerUID != "learner-uid" || state.Intent.LearnerWAL != "") {
					t.Fatalf("probe eligibility discarded the captured learner UID: %+v", state.Intent)
				}
			}
			if got, want := state.Intent.NoQuorumSince.IsZero(), wantLive; got != want {
				t.Fatalf("NoQuorumSince cleared=%v, want %v", got, want)
			}
			if !tc.unbound && !tc.recreated && probes == 0 {
				t.Error("no active quorum probe was performed")
			}
			if tc.unknown || tc.unbound || tc.recreated || identityMismatch {
				if r.Status.Phase != "Blocked" || !state.Intent.NoQuorumSince.Equal(since) {
					t.Fatalf("unknown or mismatched fencing scope advanced policy: %+v since=%v probes=%d", r.Status, state.Intent.NoQuorumSince, probes)
				}
			} else if invalidProbe {
				if r.Status.Phase != "Blocked" || !state.Intent.NoQuorumSince.Equal(since) {
					t.Fatalf("invalid probe counted as outage: %+v since=%v probes=%d", r.Status, state.Intent.NoQuorumSince, probes)
				}
			} else if tc.expired {
				if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "certified archive unavailable") {
					t.Fatalf("archive guard bypassed: %+v", r.Status)
				}
			} else if tc.noTimer {
				if tc.live {
					if !state.Intent.NoQuorumSince.IsZero() {
						t.Fatal("fresh positive round retained no-quorum policy timer")
					}
				} else if state.Intent.NoQuorumSince.IsZero() {
					t.Fatal("completed no-positive probe round did not start policy timer")
				}
			} else if !wantLive && r.Status.Phase != "Recovering" {
				t.Fatalf("no-quorum timer did not proceed: %+v", r.Status)
			}
		})
	}
}
