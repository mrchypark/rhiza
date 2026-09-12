package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// --- STS fixture ---

func learnerSTS(t *testing.T) object {
	t.Helper()
	return object{
		"metadata": object{"name": "rhiza", "uid": "sts-uid"},
		"spec": object{
			"volumeClaimTemplates": []any{},
			"template": object{"spec": object{
				"containers": []any{object{
					"name": "rhiza",
					"ports": []any{
						object{"name": "http", "containerPort": float64(8080)},
						object{"name": "peer-quic", "containerPort": float64(9090), "protocol": "UDP"},
					},
					"env": []any{
						object{"name": "RHIZA_CLUSTER_ID", "value": "source"},
						object{"name": "RHIZA_ADMIN_TOKEN", "value": "admin"},
					},
					"volumeMounts": []any{object{"name": "data", "mountPath": "/data"}},
				}},
				"volumes": []any{object{"name": "data", "emptyDir": object{}}},
			}},
		},
	}
}

// --- Fake K8s API ---

type learnerAPI struct {
	mu              sync.Mutex
	server          *httptest.Server
	secret          object
	service         object
	pod             object
	secretExists    bool
	serviceExists   bool
	podExists       bool
	podDeleting     bool
	serviceBadOwner bool
	createCount     int
}

func newLearnerAPI(t *testing.T) *learnerAPI {
	t.Helper()
	a := &learnerAPI{}
	a.server = httptest.NewTLSServer(http.HandlerFunc(a.serve))
	return a
}

func (a *learnerAPI) Close() { a.server.Close() }

func (a *learnerAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")

	// GET /secrets/{name}
	if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/default/secrets/") {
		if a.secretExists && a.secret != nil {
			json.NewEncoder(w).Encode(a.secret)
			return
		}
		w.WriteHeader(404)
		return
	}
	// POST /secrets
	if r.Method == "POST" && r.URL.Path == "/api/v1/namespaces/default/secrets" {
		var next object
		if json.NewDecoder(r.Body).Decode(&next) == nil {
			a.secret = next
			a.secretExists = true
			a.createCount++
			json.NewEncoder(w).Encode(next)
			return
		}
	}

	// GET /services/{name}
	if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/default/services/") {
		if a.serviceExists && a.service != nil {
			if a.serviceBadOwner {
				meta := a.service["metadata"].(object)
				ann := meta["annotations"].(object)
				ann[ownerAnnotation] = "other-op"
			}
			json.NewEncoder(w).Encode(a.service)
			return
		}
		w.WriteHeader(404)
		return
	}
	// POST /services
	if r.Method == "POST" && r.URL.Path == "/api/v1/namespaces/default/services" {
		var next object
		if json.NewDecoder(r.Body).Decode(&next) == nil {
			a.service = next
			a.serviceExists = true
			a.createCount++
			json.NewEncoder(w).Encode(next)
			return
		}
	}

	// GET /pods/{name}
	if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/default/pods/") {
		if a.podExists && a.pod != nil {
			if a.podDeleting {
				meta := a.pod["metadata"].(object)
				meta["deletionTimestamp"] = "2025-01-01T00:00:00Z"
			}
			json.NewEncoder(w).Encode(a.pod)
			return
		}
		w.WriteHeader(404)
		return
	}
	// POST /pods
	if r.Method == "POST" && r.URL.Path == "/api/v1/namespaces/default/pods" {
		var next object
		if json.NewDecoder(r.Body).Decode(&next) == nil {
			a.pod = next
			a.podExists = true
			a.createCount++
			next["status"] = object{"podIP": "10.0.0.1"}
			json.NewEncoder(w).Encode(next)
			return
		}
	}

	http.NotFound(w, r)
}

func (a *learnerAPI) kube(t *testing.T) *Kubernetes {
	t.Helper()
	token := filepath.Join(t.TempDir(), "token")
	os.WriteFile(token, []byte("token"), 0600)
	return &Kubernetes{BaseURL: a.server.URL, Namespace: "default", TokenFile: token, Client: a.server.Client()}
}

func learnerFixture(t *testing.T) (context.Context, *learnerAPI, *Controller) {
	t.Helper()
	api := newLearnerAPI(t)
	t.Cleanup(api.Close)
	return context.Background(), api, &Controller{Kube: api.kube(t)}
}

// --- Tests ---

func TestEnsureAutomaticLearnerCreatesAllResources(t *testing.T) {
	ctx, api, c := learnerFixture(t)
	m, err := c.ensureAutomaticLearner(ctx, learnerSTS(t), "rhiza", "op-1", "l-a")
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != "l-a" || m.URL == "" || m.PeerURL == "" || m.Token == "" {
		t.Fatalf("incomplete member: %+v", m)
	}
	if !api.secretExists || !api.serviceExists || !api.podExists {
		t.Fatalf("secret=%v service=%v pod=%v", api.secretExists, api.serviceExists, api.podExists)
	}
	if api.secret["immutable"] != true {
		t.Fatal("secret not immutable")
	}
	if api.service["spec"].(object)["clusterIP"] != "None" {
		t.Fatal("service not headless")
	}
	if nested(api.service, "spec", "publishNotReadyAddresses") != true {
		t.Fatal("learner DNS must resolve before voting readiness")
	}
	ctr := asObject(list(nested(api.pod, "spec", "containers"))[0])
	found := false
	for _, item := range list(ctr["env"]) {
		entry := asObject(item)
		if str(entry["name"]) == "RHIZA_LEARNER" {
			found = true
			if nested(entry, "valueFrom", "secretKeyRef", "name") != "l-a-credentials" || nested(entry, "valueFrom", "secretKeyRef", "key") != "member" || entry["value"] != nil {
				t.Fatal("learner credential must remain a Secret reference")
			}
		}
	}
	if !found {
		t.Fatal("learner environment missing")
	}
	podMeta := api.pod["metadata"].(object)
	labels := podMeta["labels"].(object)
	if labels["rhiza.mrchypark.dev/automatic-learner"] != "l-a" {
		t.Fatal("pod label mismatch")
	}
	ann := podMeta["annotations"].(object)
	if ann[ownerAnnotation] != "op-1" {
		t.Fatal("pod owner mismatch")
	}
	if api.pod["spec"].(object)["restartPolicy"] != "Always" {
		t.Fatal("restartPolicy not Always")
	}
}

// TestReuseAcrossFreshControllers catches the bug where a new random token
// on retry causes the stored secret to never match.
func TestReuseAcrossFreshControllers(t *testing.T) {
	ctx, api, c := learnerFixture(t)
	m1, err := c.ensureAutomaticLearner(ctx, learnerSTS(t), "rhiza", "op-1", "l-reuse")
	if err != nil {
		t.Fatal(err)
	}
	// New controller, same API state (secret exists).
	c2 := &Controller{Kube: api.kube(t)}
	m2, err := c2.ensureAutomaticLearner(ctx, learnerSTS(t), "rhiza", "op-1", "l-reuse")
	if err != nil {
		t.Fatal(err)
	}
	if m1.Token != m2.Token {
		t.Fatalf("tokens differ across controllers: %q vs %q", m1.Token, m2.Token)
	}
	if m1.URL != m2.URL || m1.PeerURL != m2.PeerURL {
		t.Fatalf("endpoints differ: %+v vs %+v", m1, m2)
	}
	if api.createCount != 3 {
		t.Fatalf("expected 3 creates, got %d", api.createCount)
	}
}

func TestRejectsDifferentOwner(t *testing.T) {
	ctx := context.Background()
	api0 := newLearnerAPI(t)
	defer api0.Close()
	c0 := &Controller{Kube: api0.kube(t)}
	_, err := c0.ensureAutomaticLearner(ctx, learnerSTS(t), "rhiza", "op-1", "l-own")
	if err != nil {
		t.Fatal(err)
	}
	// Second call with different owner on the same name.
	api := &learnerAPI{server: httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/default/secrets/") {
			meta := object{"name": "l-own-credentials", "annotations": object{ownerAnnotation: "op-1"}}
			data := base64.StdEncoding.EncodeToString(jsonBytes(quepaxa.Member{ID: "l-own", URL: "http://l-own:8080", PeerURL: "quic://l-own:9090", Token: "tok"}))
			json.NewEncoder(w).Encode(object{"immutable": true, "metadata": meta, "data": object{"member": data}})
			return
		}
		http.NotFound(w, r)
	}))}
	defer api.Close()
	c2 := &Controller{Kube: api.kube(t)}
	_, err = c2.ensureAutomaticLearner(ctx, learnerSTS(t), "rhiza", "op-2", "l-own")
	if err == nil || !strings.Contains(err.Error(), "another operation") {
		t.Fatalf("expected owner conflict, got: %v", err)
	}
}

func TestRejectsDeletingPod(t *testing.T) {
	ctx, api, c := learnerFixture(t)
	_, err := c.ensureAutomaticLearner(ctx, learnerSTS(t), "rhiza", "op-1", "l-del")
	if err != nil {
		t.Fatal(err)
	}
	api.podDeleting = true
	_, err = c.ensureAutomaticLearner(ctx, learnerSTS(t), "rhiza", "op-1", "l-del")
	if err == nil || !strings.Contains(err.Error(), "being deleted") {
		t.Fatalf("expected deleting error, got: %v", err)
	}
}

func TestRejectsNonImmutableSecret(t *testing.T) {
	ctx := context.Background()
	sts := learnerSTS(t)
	// Seed an existing mutable secret.
	api := &learnerAPI{server: httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/default/secrets/") {
			json.NewEncoder(w).Encode(object{"immutable": false, "metadata": object{"name": "l-mut-credentials", "annotations": object{ownerAnnotation: "op-1"}}, "data": object{"member": base64.StdEncoding.EncodeToString(jsonBytes(quepaxa.Member{ID: "l-mut", URL: "http://l-mut:8080", PeerURL: "quic://l-mut:9090", Token: "tok"}))}})
			return
		}
		http.NotFound(w, r)
	}))}
	defer api.Close()
	c2 := &Controller{Kube: api.kube(t)}
	_, err := c2.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "l-mut")
	if err == nil || !strings.Contains(err.Error(), "not immutable") {
		t.Fatalf("expected immutable error, got: %v", err)
	}
}

func TestRejectsCredentialMismatch(t *testing.T) {
	ctx := context.Background()
	sts := learnerSTS(t)
	wrongData := base64.StdEncoding.EncodeToString(jsonBytes(quepaxa.Member{ID: "wrong-id", URL: "http://wrong", PeerURL: "quic://wrong", Token: "tok"}))
	api := &learnerAPI{server: httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/v1/namespaces/default/secrets/") {
			json.NewEncoder(w).Encode(object{"immutable": true, "metadata": object{"name": "l-mismatch-credentials", "annotations": object{ownerAnnotation: "op-1"}}, "data": object{"member": wrongData}})
			return
		}
		http.NotFound(w, r)
	}))}
	defer api.Close()
	c2 := &Controller{Kube: api.kube(t)}
	_, err := c2.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "l-mismatch")
	if err == nil || !strings.Contains(err.Error(), "do not match") {
		t.Fatalf("expected mismatch error, got: %v", err)
	}
}

func TestRejectsHostNetwork(t *testing.T) {
	ctx, _, c := learnerFixture(t)
	sts := learnerSTS(t)
	asObject(nested(sts, "spec", "template", "spec"))["hostNetwork"] = true
	_, err := c.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "l-hn")
	if err == nil || !strings.Contains(err.Error(), "hostNetwork") {
		t.Fatalf("expected hostNetwork error, got: %v", err)
	}
}

func TestRejectsSidecar(t *testing.T) {
	ctx, _, c := learnerFixture(t)
	sts := learnerSTS(t)
	containers := list(nested(sts, "spec", "template", "spec", "containers"))
	containers = append(containers, object{"name": "sidecar"})
	asObject(nested(sts, "spec", "template", "spec"))["containers"] = containers
	_, err := c.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "l-sc")
	if err == nil || !strings.Contains(err.Error(), "exactly one container") {
		t.Fatalf("expected sidecar error, got: %v", err)
	}
}

func TestRejectsMissingHTTPPort(t *testing.T) {
	ctx, _, c := learnerFixture(t)
	sts := learnerSTS(t)
	ctr, _ := container(sts, "rhiza")
	ctr["ports"] = []any{asObject(list(ctr["ports"])[1])} // only peer-quic
	_, err := c.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "l-np")
	if err == nil || !strings.Contains(err.Error(), "http") {
		t.Fatalf("expected http port error, got: %v", err)
	}
}

func TestRejectsUDPHTTPPort(t *testing.T) {
	ctx, _, c := learnerFixture(t)
	sts := learnerSTS(t)
	ctr, _ := container(sts, "rhiza")
	ctr["ports"] = []any{
		object{"name": "http", "containerPort": float64(8080), "protocol": "UDP"},
		object{"name": "peer-quic", "containerPort": float64(9090), "protocol": "UDP"},
	}
	_, err := c.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "l-udp")
	if err == nil || !strings.Contains(err.Error(), "http port must be TCP") {
		t.Fatalf("expected TCP error, got: %v", err)
	}
}

func TestRejectsTCPPeerPort(t *testing.T) {
	ctx, _, c := learnerFixture(t)
	sts := learnerSTS(t)
	ctr, _ := container(sts, "rhiza")
	ctr["ports"] = []any{
		object{"name": "http", "containerPort": float64(8080)},
		object{"name": "peer-quic", "containerPort": float64(9090), "protocol": "TCP"},
	}
	_, err := c.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "l-tcp")
	if err == nil || !strings.Contains(err.Error(), "peer-quic port must be UDP") {
		t.Fatalf("expected UDP error, got: %v", err)
	}
}

func TestRejectsPVC(t *testing.T) {
	ctx, _, c := learnerFixture(t)
	sts := learnerSTS(t)
	asObject(sts["spec"])["volumeClaimTemplates"] = []any{object{"metadata": object{"name": "data"}}}
	_, err := c.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "l-pvc")
	if err == nil || !strings.Contains(err.Error(), "volumeClaimTemplates") {
		t.Fatalf("expected PVC error, got: %v", err)
	}
}

func TestRejectsServiceBadOwner(t *testing.T) {
	ctx, api, c := learnerFixture(t)
	_, err := c.ensureAutomaticLearner(ctx, learnerSTS(t), "rhiza", "op-1", "l-svc")
	if err != nil {
		t.Fatal(err)
	}
	api.serviceBadOwner = true
	_, err = c.ensureAutomaticLearner(ctx, learnerSTS(t), "rhiza", "op-1", "l-svc")
	if err == nil || !strings.Contains(err.Error(), "another operation") {
		t.Fatalf("expected owner conflict, got: %v", err)
	}
}

func TestPreservesReadinessProbe(t *testing.T) {
	ctx, api, c := learnerFixture(t)
	sts := learnerSTS(t)
	ctr, _ := container(sts, "rhiza")
	ctr["readinessProbe"] = object{"httpGet": object{"path": "/ready", "port": float64(8080)}}
	ctr["livenessProbe"] = object{"httpGet": object{"path": "/healthz", "port": float64(8080)}}
	_, err := c.ensureAutomaticLearner(ctx, sts, "rhiza", "op-1", "l-prb")
	if err != nil {
		t.Fatal(err)
	}
	podCtr := asObject(list(api.pod["spec"].(object)["containers"])[0])
	if _, ok := podCtr["readinessProbe"]; !ok {
		t.Fatal("readinessProbe should be preserved")
	}
	if _, ok := podCtr["livenessProbe"]; !ok {
		t.Fatal("livenessProbe should be preserved")
	}
}
