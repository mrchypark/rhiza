package operator

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type replicaScaleTestServer struct {
	t          *testing.T
	mu         sync.Mutex
	cluster    map[string]any
	deployment map[string]any
	scalePuts  int
	statusPuts int
	scaleGets  int
	proof      func(map[string]any)
	proofReady bool
}

func (s *replicaScaleTestServer) roundTrip(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/scale") {
		s.scaleGets++
		s.t.Fatal("observer fetched the Scale subresource")
	}
	if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/deployments/") {
		return replicaScaleResponse(200, s.deployment), nil
	}
	if r.Method == http.MethodGet {
		if s.proofReady && s.proof != nil {
			s.proof(s.cluster)
			meta := asObject(s.cluster["metadata"])
			meta["resourceVersion"] = replicaScaleNextVersion(s.t, str(meta["resourceVersion"]))
			s.proofReady = false
		}
		return replicaScaleResponse(200, s.cluster), nil
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.t.Fatal(err)
	}
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		s.t.Fatal("invalid request JSON")
	}
	if strings.HasSuffix(r.URL.Path, "/scale") && r.Method == http.MethodPut {
		meta, spec := asObject(value["metadata"]), asObject(value["spec"])
		depMeta := asObject(s.deployment["metadata"])
		if meta["uid"] != depMeta["uid"] || meta["resourceVersion"] != depMeta["resourceVersion"] {
			return replicaScaleResponse(409, map[string]any{}), nil
		}
		desired := int32(number(spec["replicas"]))
		asObject(s.deployment["spec"])["replicas"] = float64(desired)
		rv := replicaScaleNextVersion(s.t, str(depMeta["resourceVersion"]))
		depMeta["resourceVersion"], meta["resourceVersion"] = rv, rv
		value["status"] = map[string]any{"replicas": asObject(s.deployment["status"])["replicas"]}
		s.scalePuts++
		return replicaScaleResponse(200, value), nil
	}
	if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/status") {
		meta := asObject(value["metadata"])
		if meta["resourceVersion"] != asObject(s.cluster["metadata"])["resourceVersion"] {
			return replicaScaleResponse(409, map[string]any{}), nil
		}
		oldCheckID := str(nested(s.cluster, "status", "replicaCheck", "id"))
		newCheckID := str(nested(value, "status", "replicaCheck", "id"))
		meta["resourceVersion"] = replicaScaleNextVersion(s.t, str(meta["resourceVersion"]))
		s.cluster = value
		if newCheckID != "" && newCheckID != oldCheckID && s.proof != nil {
			s.proofReady = true
		}
		s.statusPuts++
		return replicaScaleResponse(200, s.cluster), nil
	}
	s.t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
	return nil, nil
}

func replicaScaleResponse(code int, value any) *http.Response {
	return phase2Response(code, string(mustManagedJSON(value)))
}

func replicaScaleNextVersion(t *testing.T, value string) string {
	t.Helper()
	n, err := strconv.Atoi(value)
	if err != nil {
		t.Fatal(err)
	}
	return strconv.Itoa(n + 1)
}

func replicaScaleFixture(t *testing.T) (map[string]any, V1Alpha2AuthorityView, string) {
	t.Helper()
	server, store, name := phase8Setup(t, phase8Cluster, "")
	if _, err := phase8Initialize(context.Background(), store, phase2Ref, name); err != nil {
		t.Fatal(err)
	}
	server.cluster = strings.Replace(server.cluster, `"resourceVersion":"48"`, `"resourceVersion":"47"`, 1)
	initial, err := LoadV1Alpha2ApplicationInit(context.Background(), store, phase2Ref)
	if err != nil {
		t.Fatal(err)
	}
	consumed, observation, err := CommitV1Alpha2ApplicationInit(context.Background(), store, initial, initial.Assignment().Plan, json.RawMessage(`{"schema":"empty"}`))
	if err != nil || observation != V1Alpha2CASApplied {
		t.Fatalf("consume initialization: %s %v", observation, err)
	}
	cluster := jsonDecode(t, server.cluster)
	// A flipped HPA proposal must not strand the already-authorized 3 -> 4 step.
	asObject(cluster["spec"])["replicas"] = 3
	return cluster, consumed.Authority(), name
}

func replicaScaleDeployment(name string) map[string]any {
	labels := map[string]any{v1alpha2ReplicaClusterLabel: phase2Ref.UID, v1alpha2ReplicaGenerationLabel: "0"}
	return map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": name, "namespace": phase2Ref.Namespace, "uid": "deployment", "resourceVersion": "91", "generation": 2,
			"labels": labels, "ownerReferences": []any{map[string]any{"apiVersion": V1Alpha2APIVersion, "kind": "RhizaCluster", "name": phase2Ref.Name, "uid": phase2Ref.UID, "controller": true}},
		},
		"spec":   map[string]any{"replicas": 3, "selector": map[string]any{"matchLabels": labels}, "template": map[string]any{"metadata": map[string]any{"labels": labels}}},
		"status": map[string]any{"replicas": 3, "readyReplicas": 2, "updatedReplicas": 3, "observedGeneration": 2},
	}
}

func replicaScaleServerFor(t *testing.T, cluster map[string]any, name string) (*replicaScaleTestServer, *V1Alpha2StatusStore) {
	t.Helper()
	s := &replicaScaleTestServer{t: t, cluster: cluster, deployment: replicaScaleDeployment(name)}
	return s, phase2Store(t, s.roundTrip)
}

func TestObserveV1Alpha2ReplicaDeploymentReturnsScaleFacts(t *testing.T) {
	cluster, authority, name := replicaScaleFixture(t)
	s, store := replicaScaleServerFor(t, cluster, name)
	got, err := ObserveV1Alpha2ReplicaDeployment(context.Background(), store, phase2Ref, authority)
	if err != nil {
		t.Fatal(err)
	}
	wantSelector := "rhiza.mrchypark.dev/cluster-uid=uid,rhiza.mrchypark.dev/generation=0"
	if got.DesiredReplicas != 3 || got.ObservedReplicas != 3 || got.ReadyReplicas != 2 || got.UpdatedReplicas != 3 || got.DeploymentGeneration != 2 || got.ObservedDeploymentGeneration != 2 || got.DeploymentResourceVersion != "91" || got.Selector != wantSelector || s.scaleGets != 0 {
		t.Fatalf("observation = %+v scale GETs=%d", got, s.scaleGets)
	}
	asObject(asObject(asObject(s.deployment["metadata"])["labels"]))[v1alpha2ReplicaGenerationLabel] = "1"
	if _, err := ObserveV1Alpha2ReplicaDeployment(context.Background(), store, phase2Ref, authority); err == nil {
		t.Fatal("Deployment label change was accepted")
	}
}

func TestReconcileV1Alpha2ReplicaScaleAppliesOneRVGuard(t *testing.T) {
	cluster, authority, name := replicaScaleFixture(t)
	status := asObject(cluster["status"])
	expires := time.Now().Add(time.Hour)
	status["replicaStep"] = V1Alpha2ReplicaStep{Generation: authority.Generation, DeploymentUID: authority.Binding.DeploymentUID, DeploymentResourceVersion: "91", ConfigID: 1, From: 3, To: 4, ExpiresAt: expires}
	s, store := replicaScaleServerFor(t, cluster, name)
	if err := ReconcileV1Alpha2ReplicaScale(context.Background(), store, phase2Ref); err != nil {
		t.Fatal(err)
	}
	if s.scalePuts != 1 || s.statusPuts != 1 || number(nested(s.deployment, "spec", "replicas")) != 4 {
		t.Fatalf("scale writes=%d status writes=%d replicas=%v", s.scalePuts, s.statusPuts, nested(s.deployment, "spec", "replicas"))
	}
	reported := asObject(asObject(s.cluster["status"]))
	if number(reported["replicas"]) != 3 || reported["selector"] != "rhiza.mrchypark.dev/cluster-uid=uid,rhiza.mrchypark.dev/generation=0" || reported["replicaStep"] == nil {
		t.Fatalf("reported scale status = %+v", reported)
	}
	if err := ReconcileV1Alpha2ReplicaScale(context.Background(), store, phase2Ref); err != nil {
		t.Fatal(err)
	}
	if s.scalePuts != 1 || s.statusPuts != 1 {
		t.Fatalf("completed step was replayed: scale writes=%d status writes=%d", s.scalePuts, s.statusPuts)
	}
}

func replicaScaleDownServerFor(t *testing.T, cluster map[string]any, authority V1Alpha2AuthorityView, name string) (*replicaScaleTestServer, *V1Alpha2StatusStore) {
	t.Helper()
	step := V1Alpha2ReplicaStep{Generation: authority.Generation, DeploymentUID: authority.Binding.DeploymentUID, DeploymentResourceVersion: "90", ConfigID: 1, From: 4, To: 3, ExpiresAt: time.Now().Add(-time.Minute)}
	status := asObject(cluster["status"])
	status["replicaStep"] = step
	s, store := replicaScaleServerFor(t, cluster, name)
	asObject(s.deployment["spec"])["replicas"] = 4
	deploymentStatus := asObject(s.deployment["status"])
	deploymentStatus["replicas"] = 4
	deploymentStatus["readyReplicas"] = 4
	deploymentStatus["updatedReplicas"] = 4
	return s, store
}

func replicaScaleShortChallengeWait(t *testing.T) {
	t.Helper()
	wait, poll, ttl := v1alpha2ReplicaCheckWait, v1alpha2ReplicaCheckPoll, v1alpha2ReplicaCheckTTL
	v1alpha2ReplicaCheckWait, v1alpha2ReplicaCheckPoll, v1alpha2ReplicaCheckTTL = 40*time.Millisecond, 5*time.Millisecond, time.Second
	t.Cleanup(func() { v1alpha2ReplicaCheckWait, v1alpha2ReplicaCheckPoll, v1alpha2ReplicaCheckTTL = wait, poll, ttl })
}

func replicaScaleProveCurrentCheck(cluster map[string]any) {
	status := asObject(cluster["status"])
	check := asObject(status["replicaCheck"])
	step := asObject(status["replicaStep"])
	step["checkID"] = check["id"]
	step["deploymentResourceVersion"] = check["deploymentResourceVersion"]
	step["expiresAt"] = time.Now().Add(time.Minute)
	status["replicaStep"] = step
}

func TestReconcileV1Alpha2ReplicaDownRequiresFreshProof(t *testing.T) {
	replicaScaleShortChallengeWait(t)
	for _, tc := range []struct {
		name  string
		stale bool
	}{
		{"no proof", false},
		{"old proof does not replay", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cluster, authority, name := replicaScaleFixture(t)
			s, store := replicaScaleDownServerFor(t, cluster, authority, name)
			if tc.stale {
				status := asObject(cluster["status"])
				step := status["replicaStep"].(V1Alpha2ReplicaStep)
				step.CheckID = "old-proof"
				step.DeploymentResourceVersion = "91"
				status["replicaStep"] = step
				status["replicaCheck"] = V1Alpha2ReplicaCheck{ID: "old-proof", DeploymentResourceVersion: "91", ExpiresAt: time.Now().Add(-time.Minute)}
			}
			if err := ReconcileV1Alpha2ReplicaScale(context.Background(), store, phase2Ref); err == nil {
				t.Fatal("downscale proceeded without fresh proof")
			}
			if s.scalePuts != 0 {
				t.Fatalf("downscale wrote scale %d times", s.scalePuts)
			}
			status := asObject(asObject(s.cluster)["status"])
			if status["replicaCheck"] != nil {
				t.Fatalf("timed-out check remained: %+v", status["replicaCheck"])
			}
			step := asObject(status["replicaStep"])
			if step["checkID"] != nil && step["checkID"] != "" {
				t.Fatalf("timed-out proof remained: %+v", step["checkID"])
			}
		})
	}
}

func TestReconcileV1Alpha2ReplicaDownAppliesFreshProofOnce(t *testing.T) {
	replicaScaleShortChallengeWait(t)
	cluster, authority, name := replicaScaleFixture(t)
	s, store := replicaScaleDownServerFor(t, cluster, authority, name)
	s.proof = replicaScaleProveCurrentCheck
	if err := ReconcileV1Alpha2ReplicaScale(context.Background(), store, phase2Ref); err != nil {
		t.Fatal(err)
	}
	if s.scalePuts != 1 || number(nested(s.deployment, "spec", "replicas")) != 3 {
		t.Fatalf("scale writes=%d replicas=%v", s.scalePuts, nested(s.deployment, "spec", "replicas"))
	}
	status := asObject(asObject(s.cluster)["status"])
	step := asObject(status["replicaStep"])
	check := asObject(status["replicaCheck"])
	if step["checkID"] != check["id"] || step["deploymentResourceVersion"] != check["deploymentResourceVersion"] {
		t.Fatalf("proof was not retained: step=%+v check=%+v", step, check)
	}
	if err := ReconcileV1Alpha2ReplicaScale(context.Background(), store, phase2Ref); err != nil {
		t.Fatal(err)
	}
	if s.scalePuts != 1 {
		t.Fatalf("fresh proof was replayed: %d scale writes", s.scalePuts)
	}
}

func TestReconcileV1Alpha2ReplicaDownAlreadyCompleteIgnoresOldExpiry(t *testing.T) {
	cluster, authority, name := replicaScaleFixture(t)
	s, store := replicaScaleDownServerFor(t, cluster, authority, name)
	for _, key := range []string{"replicas", "readyReplicas", "updatedReplicas"} {
		asObject(s.deployment["status"])[key] = 3
	}
	asObject(s.deployment["spec"])["replicas"] = 3
	if err := ReconcileV1Alpha2ReplicaScale(context.Background(), store, phase2Ref); err != nil {
		t.Fatal(err)
	}
	if s.scalePuts != 0 || asObject(asObject(s.cluster["status"]))["replicaCheck"] != nil {
		t.Fatalf("completed down step was reopened: writes=%d status=%+v", s.scalePuts, s.cluster["status"])
	}
}

func TestReconcileV1Alpha2ReplicaDownRejectsExpiredProof(t *testing.T) {
	replicaScaleShortChallengeWait(t)
	cluster, authority, name := replicaScaleFixture(t)
	s, store := replicaScaleDownServerFor(t, cluster, authority, name)
	s.proof = func(cluster map[string]any) {
		replicaScaleProveCurrentCheck(cluster)
		step := asObject(asObject(cluster["status"])["replicaStep"])
		step["expiresAt"] = time.Now().Add(-time.Second)
	}
	if err := ReconcileV1Alpha2ReplicaScale(context.Background(), store, phase2Ref); err == nil {
		t.Fatal("expired proof was accepted")
	}
	if s.scalePuts != 0 {
		t.Fatalf("expired proof wrote scale %d times", s.scalePuts)
	}
}

func TestReplicaAttemptBeforeScaleHonorsDeadlineAndExpiries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	<-ctx.Done()
	check := V1Alpha2ReplicaCheck{ExpiresAt: time.Now().Add(time.Minute)}
	step := V1Alpha2ReplicaStep{ExpiresAt: time.Now().Add(time.Minute)}
	if err := replicaAttemptBeforeScale(ctx, check, step); err == nil {
		t.Fatal("expired attempt context was accepted")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	check.ExpiresAt = time.Now().Add(-time.Second)
	if err := replicaAttemptBeforeScale(ctx, check, step); err == nil {
		t.Fatal("expired check was accepted")
	}
}

func TestReconcileV1Alpha2ReplicaScaleRejectsUnsafeStep(t *testing.T) {
	base, authority, name := replicaScaleFixture(t)
	valid := V1Alpha2ReplicaStep{Generation: authority.Generation, DeploymentUID: authority.Binding.DeploymentUID, DeploymentResourceVersion: "91", ConfigID: 1, From: 3, To: 4, ExpiresAt: time.Now().Add(time.Hour)}
	tests := []struct {
		name            string
		step            V1Alpha2ReplicaStep
		observedDesired int32
		mutate          func(map[string]any)
	}{
		{"expired", func() V1Alpha2ReplicaStep { s := valid; s.ExpiresAt = time.Now().Add(-time.Second); return s }(), 0, nil},
		{"wrong revision", func() V1Alpha2ReplicaStep { s := valid; s.DeploymentResourceVersion = "90"; return s }(), 0, nil},
		{"prior count", valid, 5, nil},
		{"two replicas", func() V1Alpha2ReplicaStep { s := valid; s.To = 5; return s }(), 0, nil},
		{"zero config", func() V1Alpha2ReplicaStep { s := valid; s.ConfigID = 0; return s }(), 0, nil},
		{"authority transition", valid, 0, func(c map[string]any) {
			asObject(asObject(c["status"])["authority"])["transition"] = map[string]any{"id": "repair"}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cluster := jsonDecode(t, string(mustManagedJSON(base)))
			asObject(cluster["status"])["replicaStep"] = tc.step
			if tc.mutate != nil {
				tc.mutate(cluster)
			}
			s, store := replicaScaleServerFor(t, cluster, name)
			if tc.observedDesired != 0 {
				asObject(s.deployment["spec"])["replicas"] = tc.observedDesired
				asObject(s.deployment["status"])["replicas"] = tc.observedDesired
			}
			if err := ReconcileV1Alpha2ReplicaScale(context.Background(), store, phase2Ref); err == nil {
				t.Fatal("unsafe step accepted")
			}
			if s.scalePuts != 0 {
				t.Fatalf("unsafe step wrote scale %d times", s.scalePuts)
			}
		})
	}
}

func TestReconcileV1Alpha2ReplicaScaleRequiresOptIn(t *testing.T) {
	cluster, _, name := replicaScaleFixture(t)
	delete(asObject(cluster["spec"]), "replicas")
	s, store := replicaScaleServerFor(t, cluster, name)
	if err := ReconcileV1Alpha2ReplicaScale(context.Background(), store, phase2Ref); err != nil {
		t.Fatal(err)
	}
	if s.scalePuts != 0 || s.statusPuts != 0 {
		t.Fatalf("non-opt-in wrote scale=%d status=%d", s.scalePuts, s.statusPuts)
	}
}
