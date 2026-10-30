package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mrchypark/rhiza/internal/types"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestManagedInitializationConnected(t *testing.T) {
	history, store, name := phase8Setup(t, phase8Cluster, "")
	var deployment map[string]any
	ready := false
	podFault := ""
	posts := 0
	profile := V1Alpha2RuntimeProfile{ProfileSecretName: "rhiza-runtime", InitImage: "example/init:v1", PeerPort: 9090, EvidenceFormat: "rhiza-managed-sql/v1", Storage: types.ExecutionConfig{AdminToken: "admin-must-not-leak", ObjStoreBucket: "test", ObjStorePrefix: "managed", ObjStoreProvider: "s3", ObjStoreEndpoint: "s3.example.test", ObjStoreSecretKey: "credential-must-not-leak"}}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		respond := func(v any) { w.Header().Set("Content-Type", "application/json"); _ = json.NewEncoder(w).Encode(v) }
		switch {
		case strings.Contains(r.URL.Path, "/secrets/"):
			b, _ := json.Marshal(profile)
			respond(map[string]any{"metadata": map[string]any{"name": profile.ProfileSecretName, "namespace": "ns", "uid": "secretuid"}, "data": map[string][]byte{V1Alpha2ManagedRuntimeProfileKey: b}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/deployments"):
			posts++
			if deployment != nil {
				w.WriteHeader(409)
				return
			}
			if err := json.NewDecoder(r.Body).Decode(&deployment); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			meta := deployment["metadata"].(map[string]any)
			meta["uid"] = "deployment"
			meta["resourceVersion"] = "91"
			// Exercise the API-server's admitted defaults, not just our POST body.
			spec := deployment["spec"].(map[string]any)
			spec["progressDeadlineSeconds"] = 600
			spec["revisionHistoryLimit"] = 10
			spec["strategy"] = map[string]any{"type": "RollingUpdate", "rollingUpdate": map[string]any{"maxSurge": "25%", "maxUnavailable": "25%"}}
			template := spec["template"].(map[string]any)
			template["metadata"].(map[string]any)["creationTimestamp"] = nil
			pod := template["spec"].(map[string]any)
			pod["restartPolicy"] = "Always"
			pod["dnsPolicy"] = "ClusterFirst"
			pod["schedulerName"] = "default-scheduler"
			pod["terminationGracePeriodSeconds"] = 30
			b, _ := json.Marshal(deployment)
			history.deployment = string(b)
			respond(deployment)
		case strings.Contains(r.URL.Path, "/replicasets/"):
			respond(map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "metadata": map[string]any{"name": "rs", "namespace": "ns", "uid": "rsuid", "resourceVersion": "8", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "name": name, "uid": "deployment", "controller": true}}}})
		case strings.HasSuffix(r.URL.Path, "/pods"):
			pods := []any{}
			if ready {
				for i := 1; i <= 3; i++ {
					key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string(rune('a'+i)), 32)))
					var spec map[string]any
					_ = json.Unmarshal(mustManagedJSON(deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"]), &spec)
					main := spec["containers"].([]any)[0].(map[string]any)
					switch podFault {
					case "missing readiness":
						delete(main, "readinessProbe")
					case "liveness":
						main["livenessProbe"] = main["readinessProbe"]
					case "listen changed":
						main["args"].([]any)[8] = "127.0.0.1:8080"
					}
					pods = append(pods, map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": fmt.Sprintf("pod-%d", i), "namespace": "ns", "uid": fmt.Sprintf("poduid-%d", i), "resourceVersion": "4", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs", "uid": "rsuid", "controller": true}}}, "spec": spec, "status": map[string]any{"podIP": fmt.Sprintf("10.0.0.%d", i), "initContainerStatuses": []any{map[string]any{"name": V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": fmt.Sprintf(`{"version":1,"publicKey":%q}`, key)}}}, map[string]any{"name": V1Alpha2AwaitBootstrapContainer, "state": map[string]any{"running": map[string]any{}}}}}})
				}
			}
			respond(map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{"resourceVersion": "4"}, "items": pods})
		default:
			response, err := history.roundTrip(r)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			defer response.Body.Close()
			w.WriteHeader(response.StatusCode)
			var value any
			if err := json.NewDecoder(response.Body).Decode(&value); err != nil {
				t.Error(err)
				return
			}
			_ = json.NewEncoder(w).Encode(value)
		}
	})
	server := httptest.NewTLSServer(handler)
	defer server.Close()
	store.Kube.BaseURL = server.URL
	store.Kube.Client = server.Client()
	reconcile := func() (V1Alpha2StatusSnapshot, error) {
		return ReconcileV1Alpha2ManagedInitialization(context.Background(), store.Kube, phase2Ref, profile)
	}
	if _, err := reconcile(); !errors.Is(err, ErrV1Alpha2BootstrapPending) {
		t.Fatalf("pending: %v", err)
	}
	if deployment == nil || history.puts != 0 {
		t.Fatal("candidate not created inertly")
	}
	pod := deployment["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	main := pod["containers"].([]any)[0].(map[string]any)
	if !sameJSON(mustManagedJSON(main["command"]), json.RawMessage(`["/usr/local/bin/rhiza-ha-init"]`)) || !sameJSON(mustManagedJSON(main["args"]), json.RawMessage(`["serve-managed-sql","--runtime-profile","/etc/rhiza/managed/runtime.json","--identity-file","/var/lib/rhiza/identity.json","--handoff-file","/var/lib/rhiza/bootstrap.json","--listen","0.0.0.0:8080"]`)) {
		t.Fatal("main reference-host contract missing")
	}
	if !sameJSON(mustManagedJSON(main["readinessProbe"]), json.RawMessage(`{"exec":{"command":["/usr/local/bin/rhiza-ha-init","check-ready","--runtime-profile","/etc/rhiza/managed/runtime.json"]},"periodSeconds":5,"timeoutSeconds":2,"successThreshold":1,"failureThreshold":3}`)) || main["livenessProbe"] != nil {
		t.Fatal("private readiness-only probe missing")
	}
	ready = true
	for _, fault := range []string{"missing readiness", "liveness", "listen changed"} {
		podFault = fault
		if _, err := reconcile(); !errors.Is(err, ErrV1Alpha2BootstrapPending) || history.puts != 0 {
			t.Fatalf("accepted Pod main drift %s: %v", fault, err)
		}
	}
	podFault = ""
	got, err := reconcile()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := got.BootstrapPlan()
	if err != nil || len(plan.Members) != 3 || history.puts != 1 {
		t.Fatalf("commit: %+v %v puts=%d", plan, err, history.puts)
	}
	if strings.Contains(history.cluster, "credential-must-not-leak") || strings.Contains(string(mustManagedJSON(deployment)), "credential-must-not-leak") {
		t.Fatal("profile credential escaped Secret")
	}
	if strings.Contains(history.cluster, "admin-must-not-leak") || strings.Contains(string(mustManagedJSON(deployment)), "admin-must-not-leak") {
		t.Fatal("admin token escaped Secret")
	}
	// Advance the real assignment through its evidence CAS, then ensure reconcile
	// only reads committed history even when identities are no longer discoverable.
	// The reused history fixture advances resourceVersion 47 -> 48 per CAS.
	history.cluster = strings.Replace(history.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
	assignment, err := LoadV1Alpha2ApplicationInit(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	_, observation, err := CommitV1Alpha2ApplicationInit(context.Background(), store, assignment, assignment.Assignment().Plan, json.RawMessage(`{"schema":"empty"}`))
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("consume: %v %v", observation, err)
	}
	before := posts
	ready = false
	again, err := reconcile()
	if err != nil {
		t.Fatal(err)
	}
	after, _ := again.BootstrapPlan()
	if !sameJSON(mustManagedJSON(plan), mustManagedJSON(after)) || posts != before || history.puts != 2 {
		t.Fatal("rebuilt committed genesis")
	}
}

func mustManagedJSON(v any) json.RawMessage { b, _ := json.Marshal(v); return b }

// managedFixturePods supplies public status on exact API-owned Pods. It does
// not simulate kubelet execution or prove the init binary ran.
func managedFixturePods(ref V1Alpha2ClusterRef, rsName, rsUID string, spec json.RawMessage) []any {
	pods := make([]any, 0, 3)
	for i := 1; i <= 3; i++ {
		key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string(rune('a'+i)), 32)))
		pods = append(pods, map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": fmt.Sprintf("pod-%d", i), "namespace": ref.Namespace, "uid": fmt.Sprintf("poduid-%d", i), "resourceVersion": "4", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": rsName, "uid": rsUID, "controller": true}}}, "spec": spec, "status": map[string]any{"podIP": fmt.Sprintf("10.0.0.%d", i), "initContainerStatuses": []any{map[string]any{"name": V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": fmt.Sprintf(`{"version":1,"publicKey":%q}`, key)}}}, map[string]any{"name": V1Alpha2AwaitBootstrapContainer, "state": map[string]any{"running": map[string]any{}}}}}})
	}
	return pods
}

func TestManagedInitializationRejectsProfileBeforePost(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*V1Alpha2RuntimeProfile)
	}{
		{"unsupported provider", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreProvider = "unsupported" }},
		{"implicit provider", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreProvider = "" }},
		{"filesystem provider", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreProvider = "filesystem" }},
		{"local directory", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreDir = "/tmp/not-shared" }},
		{"async durability", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreDurability = types.ObjectStoreDurabilityAsync }},
		{"unknown durability", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreDurability = "invalid" }},
		{"no profile", func(p *V1Alpha2RuntimeProfile) { *p = V1Alpha2RuntimeProfile{} }},
		{"no admin token", func(p *V1Alpha2RuntimeProfile) { p.Storage.AdminToken = "" }},
		{"unsupported evidence format", func(p *V1Alpha2RuntimeProfile) { p.EvidenceFormat = "sql/v1" }},
		{"no Secret", func(p *V1Alpha2RuntimeProfile) { p.ProfileSecretName = "" }},
		{"invalid Secret", func(p *V1Alpha2RuntimeProfile) { p.ProfileSecretName = "../secret" }},
		{"uppercase Secret", func(p *V1Alpha2RuntimeProfile) { p.ProfileSecretName = "Secret" }},
		{"no bucket", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreBucket = "" }},
		{"no S3 endpoint", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreEndpoint = "" }},
		{"GCS endpoint", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreProvider = "gcs" }},
		{"GCS insecure", func(p *V1Alpha2RuntimeProfile) {
			p.Storage.ObjStoreProvider = "gcs"
			p.Storage.ObjStoreEndpoint = ""
			p.Storage.ObjStoreInsecure = true
		}},
		{"Azure account", func(p *V1Alpha2RuntimeProfile) { p.Storage.ObjStoreProvider = "azure"; p.Storage.ObjStoreEndpoint = "" }},
		{"Azure insecure", func(p *V1Alpha2RuntimeProfile) {
			p.Storage.ObjStoreProvider = "azure"
			p.Storage.ObjStoreEndpoint = ""
			p.Storage.ObjStoreAzureStorageAccount = "account"
			p.Storage.ObjStoreInsecure = true
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			posts := 0
			profile := V1Alpha2RuntimeProfile{ProfileSecretName: "rhiza-runtime", InitImage: "example/init:v1", PeerPort: 9090, EvidenceFormat: "rhiza-managed-sql/v1", Storage: types.ExecutionConfig{AdminToken: "private-admin", ObjStoreProvider: "s3", ObjStoreBucket: "test", ObjStoreEndpoint: "s3.example.test", ObjStorePrefix: "managed"}}
			tc.mutate(&profile)
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPost {
					posts++
					return phase2Response(500, `{}`), nil
				}
				if strings.Contains(r.URL.Path, "/secrets/") {
					b, _ := json.Marshal(profile)
					secret, _ := json.Marshal(map[string]any{"metadata": map[string]any{"name": profile.ProfileSecretName, "namespace": "ns", "uid": "secretuid"}, "data": map[string][]byte{V1Alpha2ManagedRuntimeProfileKey: b}})
					return phase2Response(200, string(secret)), nil
				}
				return phase2Response(200, phase8Cluster), nil
			})
			_, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), store.Kube, phase2Ref, profile)
			if err == nil || posts != 0 {
				t.Fatalf("invalid profile reached POST: posts=%d err=%v", posts, err)
			}
		})
	}
}

func TestManagedRuntimeProfileSupportedStorage(t *testing.T) {
	for _, storage := range []types.ExecutionConfig{
		{ObjStoreProvider: "s3", ObjStoreBucket: "bucket", ObjStoreEndpoint: "s3.example.test", ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck},
		{ObjStoreProvider: "gcs", ObjStoreBucket: "bucket"},
		{ObjStoreProvider: "azure", ObjStoreBucket: "container", ObjStoreAzureStorageAccount: "account"},
	} {
		t.Run(storage.ObjStoreProvider, func(t *testing.T) {
			storage.AdminToken = "private-admin"
			profile := V1Alpha2RuntimeProfile{ProfileSecretName: "rhiza-runtime", InitImage: "example/init:v1", PeerPort: 9090, EvidenceFormat: "rhiza-managed-sql/v1", Storage: storage}
			_, plan, err := BuildV1Alpha2RuntimePlan(phase2Ref, "logical", "deployment", profile, nil)
			if err != nil || plan.Durability != types.ObjectStoreDurabilityBeforeAck {
				t.Fatalf("supported installed profile: %v durability=%s", err, plan.Durability)
			}
		})
	}
}

func TestManagedInitializationRejectsSecretAndMainBeforePost(t *testing.T) {
	for _, name := range []string{"Secret mismatch", "Secret unavailable", "two main containers"} {
		t.Run(name, func(t *testing.T) {
			profile := V1Alpha2RuntimeProfile{ProfileSecretName: "runtime", InitImage: "reference:v1", PeerPort: 9090, EvidenceFormat: "rhiza-managed-sql/v1", Storage: types.ExecutionConfig{AdminToken: "private-admin", ObjStoreProvider: "s3", ObjStoreBucket: "bucket", ObjStoreEndpoint: "s3.example.test"}}
			cluster := phase8Cluster
			if name == "two main containers" {
				cluster = strings.Replace(cluster, `{"name":"app","image":"image"}`, `{"name":"app","image":"image"},{"name":"second","image":"image"}`, 1)
			}
			posts := 0
			store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPost {
					posts++
					return phase2Response(500, `{}`), nil
				}
				if strings.Contains(r.URL.Path, "/secrets/") {
					if name == "Secret unavailable" {
						return phase2Response(404, `{}`), nil
					}
					other := profile
					other.PeerPort++
					b, _ := json.Marshal(other)
					secret, _ := json.Marshal(map[string]any{"metadata": map[string]any{"name": "runtime", "namespace": "ns", "uid": "secretuid"}, "data": map[string][]byte{V1Alpha2ManagedRuntimeProfileKey: b}})
					return phase2Response(200, string(secret)), nil
				}
				return phase2Response(200, cluster), nil
			})
			if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), store.Kube, phase2Ref, profile); err == nil || posts != 0 {
				t.Fatalf("unsafe candidate: posts=%d err=%v", posts, err)
			}
		})
	}
}
