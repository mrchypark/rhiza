package operator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
)

const phase8Cluster = `{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"cluster","namespace":"ns","uid":"uid","resourceVersion":"47","labels":{"keep":"yes"}},"spec":{"logicalID":"logical","genesisIntent":"Create","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image"}]}}}},"status":{"unrelated":{"keep":true}}}`

func phase8Deployment(name string) string {
	return fmt.Sprintf(`{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":%q,"namespace":"ns","uid":"deployment","resourceVersion":"91","labels":{"rhiza.mrchypark.dev/cluster-uid":"uid","rhiza.mrchypark.dev/generation":"0"},"ownerReferences":[{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","name":"cluster","uid":"uid","controller":true}]}}`, name)
}

type phase8Server struct {
	t                        *testing.T
	mu                       sync.Mutex
	cluster, deployment      string
	puts                     int
	conflict, timeout, apply bool
	ready                    chan struct{}
	release                  chan struct{}
}

func (s *phase8Server) roundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPut && s.ready != nil {
		s.ready <- struct{}{}
		<-s.release
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.Contains(r.URL.Path, "/deployments/") {
		if r.Method != http.MethodGet {
			s.t.Fatalf("deployment request: %s", r.Method)
		}
		return phase2Response(200, s.deployment), nil
	}
	if r.Method == http.MethodGet {
		return phase2Response(200, s.cluster), nil
	}
	if r.Method != http.MethodPut || !strings.HasSuffix(r.URL.Path, "/status") {
		s.t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
	}
	s.puts++
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.t.Fatal(err)
	}
	var got, current map[string]json.RawMessage
	if json.Unmarshal(body, &got) != nil || json.Unmarshal([]byte(s.cluster), &current) != nil {
		s.t.Fatal("invalid JSON")
	}
	if !sameJSON(got["metadata"], current["metadata"]) {
		return phase2Response(409, "conflict"), nil
	}
	if s.conflict {
		return phase2Response(409, "conflict"), nil
	}
	if s.apply {
		s.cluster = strings.Replace(string(body), `"resourceVersion":"47"`, `"resourceVersion":"48"`, 1)
	}
	if s.timeout {
		return nil, errors.New("lost response")
	}
	return phase2Response(200, s.cluster), nil
}

func phase8Setup(t *testing.T, cluster, deployment string) (*phase8Server, *V1Alpha2StatusStore, string) {
	t.Helper()
	name, err := V1Alpha2GenesisDeploymentName(phase2Ref.UID)
	if err != nil {
		t.Fatal(err)
	}
	if deployment == "" {
		deployment = phase8Deployment(name)
	}
	s := &phase8Server{t: t, cluster: cluster, deployment: deployment, apply: true}
	return s, phase2Store(t, s.roundTrip), name
}

func phase8Initialize(ctx context.Context, store *V1Alpha2StatusStore, ref V1Alpha2ClusterRef, name string) (V1Alpha2StatusSnapshot, error) {
	plan, err := NewV1Alpha2ApplicationInitPlan(ref, "logical", "deployment", "assignment", strings.Repeat("a", 64), strings.Repeat("b", 64), "app/v1")
	if err != nil {
		return V1Alpha2StatusSnapshot{}, err
	}
	return InitializeV1Alpha2AuthorityWithPlan(ctx, store, ref, name, plan)
}

func TestPhase8GenesisAtomicAndPreservesFields(t *testing.T) {
	s, store, name := phase8Setup(t, phase8Cluster, "")
	snap, err := phase8Initialize(context.Background(), store, phase2Ref, name)
	if err != nil {
		t.Fatal(err)
	}
	if s.puts != 1 || snap.Authority().Generation != 0 || snap.Authority().Binding.DeploymentUID != "deployment" || snap.Authority().Binding.StorageID != genesisStorageID("uid") {
		t.Fatalf("bad initialization: puts=%d authority=%+v", s.puts, snap.Authority())
	}
	var before, after map[string]json.RawMessage
	_ = json.Unmarshal([]byte(phase8Cluster), &before)
	_ = json.Unmarshal([]byte(s.cluster), &after)
	for _, key := range []string{"apiVersion", "kind", "spec"} {
		if !sameJSON(before[key], after[key]) {
			t.Errorf("changed %s", key)
		}
	}
	var metadata map[string]json.RawMessage
	_ = json.Unmarshal(after["metadata"], &metadata)
	if !sameJSON(metadata["labels"], json.RawMessage(`{"keep":"yes"}`)) {
		t.Error("lost metadata labels")
	}
	var status map[string]json.RawMessage
	_ = json.Unmarshal(after["status"], &status)
	if !sameJSON(status["unrelated"], json.RawMessage(`{"keep":true}`)) {
		t.Error("lost unrelated status")
	}
	if err := validateGenesisReceipt(status["genesis"], phase2Ref, "logical"); err != nil {
		t.Fatalf("receipt: %v", err)
	}
	if _, err = phase8Initialize(context.Background(), store, phase2Ref, name); err != nil || s.puts != 1 {
		t.Fatalf("repeat changed status: puts=%d err=%v", s.puts, err)
	}
}

func TestPhase8GenesisAmbiguousAndConflict(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		conflict, timeout, apply bool
		want                     error
	}{
		{"conflict", true, false, false, ErrV1Alpha2StatusConflict},
		{"lost after apply", false, true, true, nil},
		{"lost before apply", false, true, false, ErrV1Alpha2StatusOutcomeUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, store, name := phase8Setup(t, phase8Cluster, "")
			s.conflict, s.timeout, s.apply = tc.conflict, tc.timeout, tc.apply
			_, err := phase8Initialize(context.Background(), store, phase2Ref, name)
			if (tc.want == nil && err != nil) || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestPhase8GenesisRefusesUnsafeInputs(t *testing.T) {
	name, err := V1Alpha2GenesisDeploymentName("uid")
	if err != nil {
		t.Fatal(err)
	}
	d := phase8Deployment(name)
	for _, tc := range []struct{ name, cluster, deployment string }{
		{"old object", strings.Replace(phase8Cluster, `"genesisIntent":"Create",`, "", 1), d},
		{"replaced cluster", strings.Replace(phase8Cluster, `"uid":"uid"`, `"uid":"other"`, 1), d},
		{"queue", strings.Replace(phase8Cluster, `"unrelated":`, `"recoveryQueue":{},"unrelated":`, 1), d},
		{"null queue", strings.Replace(phase8Cluster, `"unrelated":`, `"recoveryQueue":null,"unrelated":`, 1), d},
		{"pending", strings.Replace(phase8Cluster, `"unrelated":`, `"pendingWrite":null,"unrelated":`, 1), d},
		{"pending true", strings.Replace(phase8Cluster, `"unrelated":`, `"pendingWrite":true,"unrelated":`, 1), d},
		{"transition", strings.Replace(phase8Cluster, `"unrelated":`, `"transition":{},"unrelated":`, 1), d},
		{"receipt", strings.Replace(phase8Cluster, `"unrelated":`, `"receipt":{},"unrelated":`, 1), d},
		{"decision", strings.Replace(phase8Cluster, `"unrelated":`, `"decision":{},"unrelated":`, 1), d},
		{"wrong deployment UID label", phase8Cluster, strings.Replace(d, `"rhiza.mrchypark.dev/cluster-uid":"uid"`, `"rhiza.mrchypark.dev/cluster-uid":"other"`, 1)},
		{"wrong generation", phase8Cluster, strings.Replace(d, `"rhiza.mrchypark.dev/generation":"0"`, `"rhiza.mrchypark.dev/generation":"1"`, 1)},
		{"wrong owner", phase8Cluster, strings.Replace(d, `"uid":"uid","controller"`, `"uid":"other","controller"`, 1)},
		{"wrong deployment namespace", phase8Cluster, strings.Replace(d, `"namespace":"ns"`, `"namespace":"other"`, 1)},
		{"wrong deployment name", phase8Cluster, strings.Replace(d, `"name":"`+name+`"`, `"name":"other"`, 1)},
		{"deleting deployment", phase8Cluster, strings.Replace(d, `"resourceVersion":"91"`, `"deletionTimestamp":"2026-01-01T00:00:00Z","resourceVersion":"91"`, 1)},
		{"missing deployment revision", phase8Cluster, strings.Replace(d, `"resourceVersion":"91",`, "", 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.cluster == phase8Cluster && tc.deployment == d {
				t.Fatal("negative fixture did not change")
			}
			s, store, depName := phase8Setup(t, tc.cluster, tc.deployment)
			if _, err := phase8Initialize(context.Background(), store, phase2Ref, depName); err == nil {
				t.Fatal("unsafe genesis accepted")
			}
			if s.puts != 0 {
				t.Fatalf("unsafe genesis wrote %d times", s.puts)
			}
		})
	}
}

func TestPhase8GenesisLostReceiptOrAuthorityNeverResets(t *testing.T) {
	s, store, name := phase8Setup(t, phase8Cluster, "")
	if _, err := phase8Initialize(context.Background(), store, phase2Ref, name); err != nil {
		t.Fatal(err)
	}
	var obj map[string]json.RawMessage
	_ = json.Unmarshal([]byte(s.cluster), &obj)
	var status map[string]json.RawMessage
	_ = json.Unmarshal(obj["status"], &status)
	for _, key := range []string{"genesis", "authority"} {
		t.Run(key, func(t *testing.T) {
			copyStatus := make(map[string]json.RawMessage)
			for k, v := range status {
				copyStatus[k] = v
			}
			delete(copyStatus, key)
			objCopy := make(map[string]json.RawMessage)
			for k, v := range obj {
				objCopy[k] = v
			}
			objCopy["status"], _ = json.Marshal(copyStatus)
			body, _ := json.Marshal(objCopy)
			srv, st, n := phase8Setup(t, string(body), "")
			if _, err := phase8Initialize(context.Background(), st, phase2Ref, n); err == nil {
				t.Fatal("lost evidence accepted")
			}
			if _, err := st.Load(context.Background(), phase2Ref); err == nil {
				t.Fatal("lost evidence loaded")
			}
			if srv.puts != 0 {
				t.Fatal("lost evidence reset")
			}
		})
	}
	for _, key := range []string{"genesis", "authority"} {
		t.Run("corrupt "+key, func(t *testing.T) {
			copyStatus := make(map[string]json.RawMessage)
			for k, v := range status {
				copyStatus[k] = v
			}
			copyStatus[key] = json.RawMessage(`{"corrupt":true}`)
			objCopy := make(map[string]json.RawMessage)
			for k, v := range obj {
				objCopy[k] = v
			}
			objCopy["status"], _ = json.Marshal(copyStatus)
			body, _ := json.Marshal(objCopy)
			srv, st, n := phase8Setup(t, string(body), "")
			if _, err := phase8Initialize(context.Background(), st, phase2Ref, n); err == nil {
				t.Fatal("corrupt evidence accepted")
			}
			if _, err := st.Load(context.Background(), phase2Ref); err == nil {
				t.Fatal("corrupt evidence loaded")
			}
			if srv.puts != 0 {
				t.Fatal("corrupt evidence reset")
			}
		})
	}
}

func TestPhase8GenesisIntentUpdateImmutable(t *testing.T) {
	created, err := DecodeV1Alpha2Cluster([]byte(phase8Cluster))
	if err != nil {
		t.Fatal(err)
	}
	old, err := DecodeV1Alpha2Cluster([]byte(strings.Replace(phase8Cluster, `"genesisIntent":"Create",`, "", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateV1Alpha2ClusterUpdate(old, created); err == nil {
		t.Fatal("old object acquired creation intent")
	}
	if err := ValidateV1Alpha2ClusterUpdate(created, old); err == nil {
		t.Fatal("creation intent removed")
	}
	if err := ValidateV1Alpha2ClusterUpdate(created, created); err != nil {
		t.Fatalf("unchanged intent: %v", err)
	}
}

func TestPhase8GenesisRejectsNoncanonicalDeployment(t *testing.T) {
	s, store, _ := phase8Setup(t, phase8Cluster, "")
	if _, err := phase8Initialize(context.Background(), store, phase2Ref, "other"); err == nil {
		t.Fatal("arbitrary deployment accepted")
	}
	if s.puts != 0 {
		t.Fatal("arbitrary deployment wrote status")
	}
}

func TestPhase8GenesisConcurrentControllers(t *testing.T) {
	s, store, name := phase8Setup(t, phase8Cluster, "")
	s.ready = make(chan struct{}, 2)
	s.release = make(chan struct{})
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			_, err := phase8Initialize(context.Background(), store, phase2Ref, name)
			results <- err
		}()
	}
	close(start)
	<-s.ready
	<-s.ready
	close(s.release)
	successes, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			successes++
		} else if errors.Is(err, ErrV1Alpha2StatusConflict) {
			conflicts++
		} else {
			t.Fatalf("concurrent result: %v", err)
		}
	}
	if s.puts != 2 || successes != 1 || conflicts != 1 {
		t.Fatalf("puts=%d successes=%d conflicts=%d", s.puts, successes, conflicts)
	}
	if _, err := store.Load(context.Background(), phase2Ref); err != nil {
		t.Fatalf("no complete authority: %v", err)
	}
}
