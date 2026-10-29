package operator

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// phase9Server supplies API-assigned Deployment identity while phase8Server
// handles the real genesis status CAS and its resource-version checks.
type phase9Server struct {
	*phase8Server
	mu9               sync.Mutex
	posts             int
	deployment        string
	lostPost, failGet bool
	arrived           chan struct{}
	release           chan struct{}
	committed         chan struct{}
}

func TestPhase9InitializerListPaginationGuards(t *testing.T) {
	first := `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1","continue":"next"},"items":[` + phase8Cluster + `]}`
	for _, test := range []struct{ name, second string }{
		{"continuation loop", `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1","continue":"next"},"items":[]}`},
		{"version change", `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"2"},"items":[]}`},
		{"duplicate uid across pages", `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1"},"items":[` + strings.Replace(phase8Cluster, `"name":"cluster"`, `"name":"other"`, 1) + `]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _ := phase9Setup(t, phase8Cluster)
			kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/rhizaclusters") {
					if r.URL.Query().Get("continue") != "" {
						return phase2Response(200, test.second), nil
					}
					return phase2Response(200, first), nil
				}
				return s.roundTrip(r)
			}).Kube
			if err := initializeV1Alpha2List(context.Background(), kube); err == nil {
				t.Fatal("incomplete or inconsistent list accepted")
			}
			if s.posts != 0 || s.puts != 0 {
				t.Fatalf("list failure wrote: posts=%d puts=%d", s.posts, s.puts)
			}
		})
	}
}

func (s *phase9Server) roundTrip(r *http.Request) (*http.Response, error) {
	if strings.Contains(r.URL.Path, "/deployments") {
		s.mu9.Lock()
		defer s.mu9.Unlock()
		if r.Method == http.MethodPost {
			s.posts++
			if s.deployment != "" {
				if s.committed != nil {
					s.mu9.Unlock()
					<-s.committed
					s.mu9.Lock()
				}
				return phase2Response(409, "conflict"), nil
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				s.t.Fatal(err)
			}
			var obj map[string]any
			if err := json.Unmarshal(body, &obj); err != nil {
				s.t.Fatal(err)
			}
			meta := obj["metadata"].(map[string]any)
			meta["uid"], meta["resourceVersion"] = "deployment", "91"
			b, err := json.Marshal(obj)
			if err != nil {
				s.t.Fatal(err)
			}
			s.deployment = string(b)
			if s.lostPost {
				return nil, errors.New("lost create response")
			}
			return phase2Response(201, s.deployment), nil
		}
		if r.Method != http.MethodGet {
			s.t.Fatalf("unexpected deployment request: %s", r.Method)
		}
		if s.deployment == "" {
			return phase2Response(404, "not found"), nil
		}
		if s.failGet {
			return phase2Response(503, "unavailable"), nil
		}
		return phase2Response(200, s.deployment), nil
	}
	response, err := s.phase8Server.roundTrip(r)
	if s.arrived != nil && r.Method == http.MethodGet && !strings.HasSuffix(r.URL.Path, "/status") {
		select {
		case s.arrived <- struct{}{}:
			<-s.release
		default:
		}
	}
	if s.committed != nil && r.Method == http.MethodPut && err == nil && response.StatusCode == 200 {
		close(s.committed)
	}
	return response, err
}

func phase9Setup(t *testing.T, cluster string) (*phase9Server, *Kubernetes) {
	t.Helper()
	s := &phase9Server{phase8Server: &phase8Server{t: t, cluster: cluster, apply: true}}
	return s, phase2Store(t, s.roundTrip).Kube
}

func TestPhase9InitializerCreatesOnceAndPreservesStatus(t *testing.T) {
	s, kube := phase9Setup(t, phase8Cluster)
	first, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	if first.Authority().Generation != 0 || first.Authority().Binding.DeploymentUID != "deployment" || s.posts != 1 || s.puts != 1 {
		t.Fatalf("authority=%+v posts=%d puts=%d", first.Authority(), s.posts, s.puts)
	}
	var deployment struct {
		Spec struct {
			Replicas *int `json:"replicas"`
		} `json:"spec"`
	}
	if err := json.Unmarshal([]byte(s.deployment), &deployment); err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0 {
		t.Fatalf("replicas: %v", deployment.Spec.Replicas)
	}
	if _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref); err != nil {
		t.Fatal(err)
	}
	if s.posts != 1 || s.puts != 1 {
		t.Fatalf("repeat wrote: posts=%d puts=%d", s.posts, s.puts)
	}
	if !strings.Contains(s.cluster, `"unrelated":{"keep":true}`) {
		t.Fatal("unrelated status lost")
	}
}

func TestPhase9InitializerRejectsMissingIntent(t *testing.T) {
	s, kube := phase9Setup(t, strings.Replace(phase8Cluster, `"genesisIntent":"Create",`, "", 1))
	if _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref); err == nil {
		t.Fatal("missing intent accepted")
	}
	if s.posts != 0 || s.puts != 0 {
		t.Fatalf("writes: posts=%d puts=%d", s.posts, s.puts)
	}
}

func TestPhase9InitializerRejectsUnsafeExistingDeployment(t *testing.T) {
	for _, mutation := range []string{"replicas", "owner", "template", "generation", "uid"} {
		t.Run(mutation, func(t *testing.T) {
			s, kube := phase9Setup(t, phase8Cluster)
			if _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref); err != nil {
				t.Fatal(err)
			}
			var obj map[string]any
			if err := json.Unmarshal([]byte(s.deployment), &obj); err != nil {
				t.Fatal(err)
			}
			meta := obj["metadata"].(map[string]any)
			spec := obj["spec"].(map[string]any)
			switch mutation {
			case "replicas":
				spec["replicas"] = 1
			case "owner":
				meta["ownerReferences"].([]any)[0].(map[string]any)["uid"] = "other"
			case "template":
				spec["template"] = map[string]any{}
			case "generation":
				meta["labels"].(map[string]any)["rhiza.mrchypark.dev/generation"] = "1"
			case "uid":
				meta["uid"] = ""
			}
			b, err := json.Marshal(obj)
			if err != nil {
				t.Fatal(err)
			}
			if sameJSON(b, []byte(s.deployment)) {
				t.Fatal("fixture mutation did not change deployment")
			}
			s.deployment, s.cluster, s.posts, s.puts = string(b), phase8Cluster, 0, 0
			if _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref); err == nil {
				t.Fatal("unsafe deployment accepted")
			}
			if s.posts != 1 || s.puts != 0 {
				t.Fatalf("writes: posts=%d puts=%d", s.posts, s.puts)
			}
		})
	}
}

func TestPhase9InitializerCreateConflictAndLostResponse(t *testing.T) {
	for _, test := range []struct {
		name                                string
		preexisting, lost, failGet, wantErr bool
	}{
		{"conflict", true, false, false, false},
		{"lost response with proof", false, true, false, false},
		{"lost response without proof", false, true, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, kube := phase9Setup(t, phase8Cluster)
			if test.preexisting {
				if _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref); err != nil {
					t.Fatal(err)
				}
				s.cluster, s.posts, s.puts = phase8Cluster, 0, 0
			}
			s.lostPost, s.failGet = test.lost, test.failGet
			_, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref)
			if (err != nil) != test.wantErr {
				t.Fatalf("error: %v", err)
			}
			if s.posts != 1 || (test.wantErr && s.puts != 0) || (!test.wantErr && s.puts != 1) {
				t.Fatalf("posts=%d puts=%d", s.posts, s.puts)
			}
		})
	}
}

func TestPhase9InitializerImagePullDefaults(t *testing.T) {
	for _, test := range []struct{ image, policy string }{
		{"example/image", "Always"},
		{"example/image:latest", "Always"},
		{"example/image:v1", "IfNotPresent"},
		{"example/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "IfNotPresent"},
		{"registry.example:5000/image", "Always"},
	} {
		for _, valid := range []bool{true, false} {
			t.Run(test.image+"/valid="+strconv.FormatBool(valid), func(t *testing.T) {
				cluster := strings.Replace(phase8Cluster, `"image":"image"`, `"image":`+strconv.Quote(test.image), 1)
				if cluster == phase8Cluster {
					t.Fatal("cluster image mutation did not apply")
				}
				s, kube := phase9Setup(t, cluster)
				if _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref); err != nil {
					t.Fatal(err)
				}
				var obj map[string]any
				if err := json.Unmarshal([]byte(s.deployment), &obj); err != nil {
					t.Fatal(err)
				}
				container := obj["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
				policy := test.policy
				if !valid {
					if policy == "Always" {
						policy = "IfNotPresent"
					} else {
						policy = "Always"
					}
				}
				container["imagePullPolicy"] = policy
				b, err := json.Marshal(obj)
				if err != nil {
					t.Fatal(err)
				}
				if sameJSON(b, []byte(s.deployment)) {
					t.Fatal("policy mutation did not apply")
				}
				s.deployment, s.cluster, s.posts, s.puts = string(b), cluster, 0, 0
				_, err = ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref)
				if (err == nil) != valid {
					t.Fatalf("policy %s: %v", policy, err)
				}
				if s.posts != 1 || (valid && s.puts != 1) || (!valid && s.puts != 0) {
					t.Fatalf("posts=%d puts=%d", s.posts, s.puts)
				}
			})
		}
	}
}

func TestPhase9InitializerConcurrentCreateCAS(t *testing.T) {
	s, kube := phase9Setup(t, phase8Cluster)
	s.arrived, s.release, s.committed = make(chan struct{}, 2), make(chan struct{}), make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref); errs <- err }()
	}
	<-s.arrived
	<-s.arrived
	close(s.release)
	for i := 0; i < 2; i++ {
		<-errs
	}
	if s.posts != 2 || s.puts != 1 || s.deployment == "" {
		t.Fatalf("posts=%d puts=%d deployment=%q", s.posts, s.puts, s.deployment)
	}
}

func TestPhase9InitializerListGuards(t *testing.T) {
	for _, test := range []struct {
		name, body string
		wantErr    bool
	}{
		{"complete", `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1"},"items":[` + phase8Cluster + `]}`, false},
		{"missing items", `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1"}}`, true},
		{"duplicate", `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1"},"items":[` + phase8Cluster + `,` + phase8Cluster + `]}`, true},
		{"duplicate uid", `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1"},"items":[` + phase8Cluster + `,` + strings.Replace(phase8Cluster, `"name":"cluster"`, `"name":"other"`, 1) + `]}`, true},
		{"missing intent", `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1"},"items":[` + strings.Replace(phase8Cluster, `"genesisIntent":"Create",`, "", 1) + `]}`, false},
		{"partial page", `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1","continue":"next"},"items":[` + phase8Cluster + `]}`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _ := phase9Setup(t, phase8Cluster)
			kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if strings.HasSuffix(r.URL.Path, "/rhizaclusters") {
					if r.URL.Query().Get("continue") != "" {
						return phase2Response(503, "unavailable"), nil
					}
					return phase2Response(200, test.body), nil
				}
				return s.roundTrip(r)
			}).Kube
			err := initializeV1Alpha2List(context.Background(), kube)
			if (err != nil) != test.wantErr {
				t.Fatalf("error: %v", err)
			}
			if test.wantErr && (s.posts != 0 || s.puts != 0) {
				t.Fatalf("partial list wrote: posts=%d puts=%d", s.posts, s.puts)
			}
			if test.name == "complete" && (s.posts != 1 || s.puts != 1) {
				t.Fatalf("complete list: posts=%d puts=%d", s.posts, s.puts)
			}
			if test.name == "missing intent" && (s.posts != 0 || s.puts != 0) {
				t.Fatalf("missing intent wrote: posts=%d puts=%d", s.posts, s.puts)
			}
		})
	}
}

func TestPhase9InitializerNeverRecreatesLaterGeneration(t *testing.T) {
	s, kube := phase9Setup(t, phase8Cluster)
	if _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref); err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(s.cluster), &obj); err != nil {
		t.Fatal(err)
	}
	authority := obj["status"].(map[string]any)["authority"].(map[string]any)
	authority["generation"] = 2
	authority["binding"].(map[string]any)["deploymentUID"] = "later-deployment"
	b, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	s.cluster, s.deployment, s.posts, s.puts = string(b), "", 0, 0
	snap, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Authority().Generation != 2 || snap.Authority().Binding.DeploymentUID != "later-deployment" || s.posts != 0 || s.puts != 0 {
		t.Fatalf("authority=%+v posts=%d puts=%d", snap.Authority(), s.posts, s.puts)
	}
}

func TestPhase9InitializerIntervalAndCancellation(t *testing.T) {
	_, kube := phase9Setup(t, phase8Cluster)
	if err := RunV1Alpha2Initialization(context.Background(), kube, 0); err == nil {
		t.Fatal("zero interval accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := RunV1Alpha2Initialization(ctx, kube, time.Second); err == nil {
		t.Fatal("cancelled context accepted")
	}
}
