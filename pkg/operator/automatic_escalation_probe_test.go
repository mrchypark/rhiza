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

func TestAutomaticEscalationRequiresLiveQuorum(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		live, learnerOnly       bool
		corrupt                 string
		expired                 bool
		mixed, unknown, unbound bool
		recreated               bool
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
		{name: "unknown membership must block live proof", mixed: true, unknown: true, live: true},
		{name: "matching unknown membership must block live proof", unknown: true, live: true},
		{name: "unknown membership must block escalation", mixed: true, unknown: true, expired: true},
		{name: "unbound learner must block escalation", mixed: true, unbound: true, expired: true},
		{name: "recreated learner must block escalation", mixed: true, recreated: true, expired: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := validAutoControl()
			since := time.Now().UTC().Add(-time.Second)
			if tc.expired {
				since = since.Add(-time.Hour)
			}
			state.Intent = &autoIntent{Failed: "n1", Voters: state.Voters, LearnerName: "learner", Policy: AutoPolicy{FailureGraceSeconds: 600}, NoQuorumSince: since}
			if !tc.unbound {
				state.Intent.LearnerUID, state.Intent.LearnerWAL = "learner-uid", "learner-wal"
			}
			r := autoClusterResource("cluster", "uid", "logical")
			c := autoController(autoBucket(t))
			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("kube"), 0600); err != nil {
				t.Fatal(err)
			}
			probes := 0
			client := &http.Client{Transport: escalationProbeTransport(func(req *http.Request) (*http.Response, error) {
				code := 200
				var body any
				if req.Method == http.MethodPut {
					data, err := io.ReadAll(req.Body)
					if err != nil {
						return nil, err
					}
					return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(string(data))), Header: http.Header{}}, nil
				}
				if strings.Contains(req.URL.Path, "/pods/") {
					pod := req.URL.Path[strings.LastIndex(req.URL.Path, "/")+1:]
					ip := map[string]string{"rhiza-1": "127.0.0.2", "rhiza-2": "127.0.0.3", "learner": "127.0.0.4"}[pod]
					if ip == "" {
						code = 404
					}
					uid := "learner-uid"
					if tc.recreated && pod == "learner" {
						uid = "different-uid"
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
					if req.URL.Path == "/membership/status" {
						voters := []quepaxa.NodeID{"n1", "n2", "n3"}
						config := uint(2)
						if tc.learnerOnly || tc.mixed && id != "n2" {
							voters = []quepaxa.NodeID{"n2", "n3", "learner"}
							config = 3
						}
						if tc.unknown {
							voters = append(voters, "unknown-voter")
						}
						body = network.MembershipStatus{NodeID: quepaxa.NodeID(id), ClusterID: "source", ConfigID: config, Voters: voters, Voting: true}
					} else if req.URL.Path == "/recovery/probe" {
						probes++
						p := network.RecoveryProbe{Nonce: req.URL.Query().Get("nonce"), Excluded: req.URL.Query().Get("exclude"), Status: network.VoterRecoveryStatus{NodeID: id, ClusterID: "source", Quorum: tc.live}}
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
						data, _ := json.Marshal(p)
						mac := hmac.New(sha256.New, []byte("admin"))
						mac.Write(data)
						p.MAC = hex.EncodeToString(mac.Sum(nil))
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
			_, err = c.maybeEscalateAutomatic(context.Background(), r, state, version, sts)
			if err != nil {
				t.Fatal(err)
			}
			wantLive := tc.live && tc.corrupt == "" && !tc.unknown && !tc.unbound && !tc.recreated
			if got := state.Intent.NoQuorumSince.IsZero(); got != wantLive {
				t.Fatalf("NoQuorumSince cleared=%v, want %v", got, wantLive)
			}
			if !tc.unknown && !tc.unbound && !tc.recreated && probes == 0 {
				t.Error("no active quorum probe was performed")
			}
			if tc.unknown || tc.unbound || tc.recreated {
				if r.Status.Phase != "Blocked" || probes != 0 || !state.Intent.NoQuorumSince.Equal(since) {
					t.Fatalf("unknown fencing scope accepted: %+v probes=%d", r.Status, probes)
				}
			} else if tc.expired {
				if r.Status.Phase != "Blocked" || !strings.Contains(r.Status.Message, "certified archive unavailable") {
					t.Fatalf("archive guard bypassed: %+v", r.Status)
				}
			} else if !wantLive && r.Status.Phase != "Recovering" {
				t.Fatalf("no-quorum timer did not proceed: %+v", r.Status)
			}
		})
	}
}
