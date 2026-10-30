package operator

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/mrchypark/rhiza/internal/types"
	"io"
	"net/http"
	"net/http/httptest"
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
	onCreate          func()
}

func phase9RuntimeProfile() V1Alpha2RuntimeProfile {
	return V1Alpha2RuntimeProfile{InitImage: "reference:v1", PeerPort: 9090, EvidenceFormat: V1Alpha2ManagedSQLEvidenceFormat, ProfileSecretName: "runtime", Storage: types.ExecutionConfig{AdminToken: "private-admin", ObjStoreProvider: "s3", ObjStoreBucket: "bucket", ObjStoreEndpoint: "s3.example.test"}}
}

func phase9ManagedList(ctx context.Context, kube *Kubernetes) error {
	return initializeV1Alpha2ListWith(ctx, kube, func(ref V1Alpha2ClusterRef) error {
		_, err := ReconcileV1Alpha2ManagedInitialization(ctx, kube, ref, phase9RuntimeProfile())
		return err
	})
}

// Only the legacy-negative test uses these fixed digests. Managed positives
// always derive the full plan from actual API records and the canonicalizer.
func phase9Plan(_ context.Context, ref V1Alpha2ClusterRef, logicalID, deploymentUID string) (V1Alpha2ApplicationInitPlan, error) {
	return NewV1Alpha2ApplicationInitPlan(ref, logicalID, deploymentUID, "test-assignment", strings.Repeat("a", 64), strings.Repeat("b", 64), "test/v1")
}

func TestPhase9InitializerRequiresPlanBeforeCreate(t *testing.T) {
	s, kube := phase9Setup(t, phase8Cluster)
	if _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref); err == nil {
		t.Fatal("missing runtime plan accepted")
	}
	if s.posts != 0 || s.puts != 0 {
		t.Fatalf("missing plan wrote: posts=%d puts=%d", s.posts, s.puts)
	}
}

func TestPhase9InitializerLegacyPlanRemainsInert(t *testing.T) {
	s, kube := phase9Setup(t, phase8Cluster)
	if _, err := ReconcileV1Alpha2Initialization(context.Background(), kube, phase2Ref, phase9Plan); err == nil {
		t.Fatal("legacy plan-only path created genesis")
	}
	var dep struct{ Spec struct{ Replicas int } }
	if json.Unmarshal([]byte(s.deployment), &dep) != nil || dep.Spec.Replicas != 0 || s.posts != 1 || s.puts != 0 {
		t.Fatalf("legacy path was not inert: posts=%d puts=%d", s.posts, s.puts)
	}
}

func TestPhase9InitializerRejectsAssignmentWithoutGenesisBeforeCreate(t *testing.T) {
	cluster := strings.Replace(phase8Cluster, `"status":{`, `"status":{"applicationInit":{},`, 1)
	if cluster == phase8Cluster {
		t.Fatal("fixture has no status object")
	}
	s, kube := phase9Setup(t, cluster)
	if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err == nil {
		t.Fatal("orphaned assignment accepted")
	}
	if s.posts != 0 || s.puts != 0 {
		t.Fatalf("orphaned assignment wrote: posts=%d puts=%d", s.posts, s.puts)
	}
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
			if err := phase9ManagedList(context.Background(), kube); err == nil {
				t.Fatal("incomplete or inconsistent list accepted")
			}
			if s.posts != 0 || s.puts != 0 {
				t.Fatalf("list failure wrote: posts=%d puts=%d", s.posts, s.puts)
			}
		})
	}
}

func (s *phase9Server) roundTrip(r *http.Request) (*http.Response, error) {
	respond := func(v any) (*http.Response, error) {
		b, err := json.Marshal(v)
		return phase2Response(200, string(b)), err
	}
	if strings.Contains(r.URL.Path, "/secrets/") {
		b, _ := json.Marshal(phase9RuntimeProfile())
		return respond(map[string]any{"metadata": map[string]any{"name": "runtime", "namespace": "ns", "uid": "secretuid"}, "data": map[string][]byte{V1Alpha2ManagedRuntimeProfileKey: b}})
	}
	if strings.Contains(r.URL.Path, "/replicasets/") {
		name, _ := V1Alpha2GenesisDeploymentName(phase2Ref.UID)
		return respond(map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": "rs", "namespace": "ns", "uid": "rsuid", "resourceVersion": "8", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": name, "uid": "deployment", "controller": true}}}})
	}
	if strings.HasSuffix(r.URL.Path, "/pods") {
		s.mu9.Lock()
		defer s.mu9.Unlock()
		var deployment struct {
			Spec struct {
				Template struct{ Spec json.RawMessage }
			}
		}
		if json.Unmarshal([]byte(s.deployment), &deployment) != nil {
			return phase2Response(404, `{}`), nil
		}
		pods := managedFixturePods(phase2Ref, "rs", "rsuid", deployment.Spec.Template.Spec)
		return respond(map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{"resourceVersion": "4"}, "items": pods})
	}
	if strings.Contains(r.URL.Path, "/deployments") {
		s.mu9.Lock()
		defer s.mu9.Unlock()
		if r.Method == http.MethodPost {
			s.posts++
			if s.deployment != "" {
				if s.committed != nil {
					s.mu9.Unlock()
					select {
					case <-s.committed:
					case <-r.Context().Done():
						s.mu9.Lock()
						return nil, r.Context().Err()
					}
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
			if s.onCreate != nil {
				s.onCreate()
			}
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
			select {
			case <-s.release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
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
	kube := phase2Store(t, s.roundTrip).Kube
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		response, err := s.roundTrip(r)
		if err != nil {
			// Model an applied POST with a genuinely lost HTTP response.
			conn, _, hijackErr := w.(http.Hijacker).Hijack()
			if hijackErr == nil {
				_ = conn.Close()
			}
			return
		}
		defer response.Body.Close()
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	}))
	t.Cleanup(server.Close)
	kube.BaseURL = server.URL
	kube.Client = server.Client()
	kube.Client.Timeout = 5 * time.Second
	return s, kube
}

func TestPhase9InitializerCreatesOnceAndPreservesStatus(t *testing.T) {
	s, kube := phase9Setup(t, phase8Cluster)
	first, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile())
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
	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 3 {
		t.Fatalf("replicas: %v", deployment.Spec.Replicas)
	}
	if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err != nil {
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
	if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err == nil {
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
			if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err != nil {
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
			if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err == nil {
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
				if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err != nil {
					t.Fatal(err)
				}
				s.cluster, s.posts, s.puts = phase8Cluster, 0, 0
			}
			s.lostPost, s.failGet = test.lost, test.failGet
			_, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile())
			wantPosts := 1
			if test.lost {
				if err == nil || s.deployment == "" || s.puts != 0 {
					t.Fatalf("uncertain POST did not preserve gated candidate: %v", err)
				}
				// The managed path returns uncertainty immediately. The next poll
				// proves the same live candidate instead of deleting/recreating it.
				s.lostPost = false
				_, err = ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile())
				wantPosts = 2
			}
			if (err != nil) != test.wantErr {
				t.Fatalf("error: %v", err)
			}
			if s.posts != wantPosts || (test.wantErr && s.puts != 0) || (!test.wantErr && s.puts != 1) {
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
				if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err != nil {
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
				_, err = ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile())
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		go func() {
			_, err := ReconcileV1Alpha2ManagedInitialization(ctx, kube, phase2Ref, phase9RuntimeProfile())
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-s.arrived:
		case <-ctx.Done():
			close(s.release)
			t.Fatal("concurrent initial GET barrier timed out")
		}
	}
	close(s.release)
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Errorf("concurrent reconcile: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent reconciliation timed out")
		}
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
			err := phase9ManagedList(context.Background(), kube)
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
	if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(s.cluster), &obj); err != nil {
		t.Fatal(err)
	}
	authority := obj["status"].(map[string]any)["authority"].(map[string]any)
	assignment := obj["status"].(map[string]any)["applicationInit"].(map[string]any)
	assignment["state"] = "Consumed"
	assignment["initialEvidenceDigest"] = strings.Repeat("a", 64)
	authority["evidence"] = map[string]any{"tip": 7}
	authority["generation"] = 2
	authority["binding"].(map[string]any)["deploymentUID"] = "later-deployment"
	b, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	s.cluster, s.deployment, s.posts, s.puts = string(b), "", 0, 0
	snap, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile())
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
	if err := RunV1Alpha2ManagedInitialization(context.Background(), kube, 0, phase9RuntimeProfile()); err == nil {
		t.Fatal("managed zero interval accepted")
	}
	if err := RunV1Alpha2ManagedInitialization(ctx, kube, time.Second, phase9RuntimeProfile()); !errors.Is(err, context.Canceled) {
		t.Fatalf("managed cancellation: %v", err)
	}
}

func TestPhase9InitializerGenesisCASConflictAndLostResponse(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		conflict, timeout, apply, wantErr bool
	}{
		{"CAS conflict", true, false, true, true},
		{"applied response lost", false, true, true, false},
		{"unapplied response lost", false, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, kube := phase9Setup(t, phase8Cluster)
			s.conflict, s.timeout, s.apply = tc.conflict, tc.timeout, tc.apply
			_, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile())
			if (err != nil) != tc.wantErr || s.puts != 1 {
				t.Fatalf("CAS outcome: %v puts=%d", err, s.puts)
			}
			if tc.conflict && !errors.Is(err, ErrV1Alpha2StatusConflict) {
				t.Fatalf("lost conflict type: %v", err)
			}
			if tc.timeout && !tc.apply && !errors.Is(err, ErrV1Alpha2StatusOutcomeUnknown) {
				t.Fatalf("lost unknown outcome type: %v", err)
			}
			if s.deployment == "" {
				t.Fatal("uncertain candidate removed")
			}
			s.conflict, s.timeout, s.apply = false, false, true
			got, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile())
			if err != nil {
				t.Fatal(err)
			}
			p, err := got.BootstrapPlan()
			if err != nil || len(p.Members) != 3 {
				t.Fatalf("full committed plan: %v", err)
			}
			wantPuts := 2
			if !tc.wantErr {
				wantPuts = 1
			}
			if s.puts != wantPuts {
				t.Fatalf("genesis recreated after readback: puts=%d", s.puts)
			}
		})
	}
}

func TestPhase9InitializerRefusesSpecChangeBeforeCAS(t *testing.T) {
	s, kube := phase9Setup(t, phase8Cluster)
	s.onCreate = func() { s.cluster = strings.Replace(s.cluster, `"image":"image"`, `"image":"changed"`, 1) }
	if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err == nil || s.puts != 0 {
		t.Fatalf("changed spec accepted: %v puts=%d", err, s.puts)
	}
}

func TestPhase9InitializerManagedLoopCommitsAndCancels(t *testing.T) {
	s, _ := phase9Setup(t, phase8Cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/rhizaclusters") {
			return phase2Response(200, `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaClusterList","metadata":{"resourceVersion":"1"},"items":[`+phase8Cluster+`]}`), nil
		}
		response, err := s.roundTrip(r)
		if r.Method == http.MethodPut && err == nil && response.StatusCode == 200 {
			cancel()
		}
		return response, err
	}).Kube
	if err := RunV1Alpha2ManagedInitialization(ctx, kube, time.Hour, phase9RuntimeProfile()); !errors.Is(err, context.Canceled) {
		t.Fatalf("loop did not cancel: %v", err)
	}
	if s.posts != 1 || s.puts != 1 || !strings.Contains(s.cluster, `"bootstrapPlan"`) {
		t.Fatalf("managed loop did not commit once: posts=%d puts=%d", s.posts, s.puts)
	}
}
