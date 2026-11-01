package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestCollectBootstrapValidOwnedPods(t *testing.T) {
	init, err := V1Alpha2BootstrapInitContainers("example/app:v1", "uid", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range init {
		c["env"].([]any)[0].(map[string]any)["valueFrom"].(map[string]any)["fieldRef"].(map[string]any)["apiVersion"] = "v1"
		c["imagePullPolicy"] = "IfNotPresent"
		c["resources"] = map[string]any{}
		c["volumeMounts"] = append(c["volumeMounts"].([]any), map[string]any{"name": "kube-api-access-abc", "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": true})
	}
	projected := map[string]any{"defaultMode": 420, "sources": []any{map[string]any{"serviceAccountToken": map[string]any{"expirationSeconds": 3607, "path": "token"}}, map[string]any{"configMap": map[string]any{"name": "kube-root-ca.crt", "items": []any{map[string]any{"key": "ca.crt", "path": "ca.crt"}}}}, map[string]any{"downwardAPI": map[string]any{"items": []any{map[string]any{"path": "namespace", "fieldRef": map[string]any{"apiVersion": "v1", "fieldPath": "metadata.namespace"}}}}}}}
	pods := make([]any, 0, 3)
	for i := 1; i <= 3; i++ {
		key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string(rune('a'+i)), 32)))
		pods = append(pods, map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": fmt.Sprintf("pod-%d", i), "namespace": "ns", "uid": fmt.Sprintf("poduid-%d", i), "resourceVersion": "4", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "rs", "uid": "rsuid", "controller": true}}}, "spec": map[string]any{"initContainers": init, "volumes": []any{map[string]any{"name": "rhiza-data", "emptyDir": map[string]any{}}, map[string]any{"name": "kube-api-access-abc", "projected": projected}}}, "status": map[string]any{"podIP": fmt.Sprintf("10.0.0.%d", i), "initContainerStatuses": []any{map[string]any{"name": V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": fmt.Sprintf(`{"version":1,"publicKey":%q}`, key)}}}, map[string]any{"name": V1Alpha2AwaitBootstrapContainer, "state": map[string]any{"running": map[string]any{"startedAt": "2026-01-01T00:00:00Z"}}}}, "containerStatuses": []any{map[string]any{"state": map[string]any{"waiting": map[string]any{}}, "restartCount": 0, "lastState": map[string]any{}}}}})
	}
	for _, raw := range pods {
		spec := raw.(map[string]any)["spec"].(map[string]any)
		spec["securityContext"] = V1Alpha2ManagedPodSecurity()
		spec["containers"] = []any{map[string]any{"name": "app", "image": "image", "volumeMounts": []any{map[string]any{"name": V1Alpha2BootstrapVolume, "mountPath": V1Alpha2BootstrapDataDir}, map[string]any{"name": "kube-api-access-abc", "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": true}}}}
	}
	depName, _ := V1Alpha2GenesisDeploymentName("uid")
	dep := phase8Deployment(depName)
	rs := `{"apiVersion":"apps/v1","kind":"ReplicaSet","metadata":{"name":"rs","namespace":"ns","uid":"rsuid","resourceVersion":"9","ownerReferences":[{"apiVersion":"apps/v1","kind":"Deployment","name":"` + depName + `","uid":"deployment","controller":true}]}}`
	kube := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		switch {
		case strings.Contains(r.URL.Path, "/rhizaclusters/"):
			return phase2Response(200, phase8Cluster), nil
		case strings.Contains(r.URL.Path, "/deployments/"):
			return phase2Response(200, dep), nil
		case strings.Contains(r.URL.Path, "/replicasets/"):
			return phase2Response(200, rs), nil
		case strings.HasSuffix(r.URL.Path, "/pods"):
			b, _ := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{"resourceVersion": "4"}, "items": pods})
			return phase2Response(200, string(b)), nil
		}
		return phase2Response(404, ""), nil
	}).Kube
	check := func() ([]V1Alpha2BootstrapMember, error) {
		return CollectV1Alpha2BootstrapMembers(context.Background(), kube, phase2Ref, depName, "deployment", "example/app:v1", 3, 9090)
	}
	got, err := check()
	if err != nil || len(got) != 3 {
		t.Fatalf("collect: %v count=%d", err, len(got))
	}
	for _, member := range got {
		id, e := types.ManagedNodeID(member.PodUID)
		var index int
		_, scanErr := fmt.Sscanf(member.PodUID, "poduid-%d", &index)
		var key [32]byte
		if index >= 1 && index <= 3 {
			copy(key[:], strings.Repeat(string(rune('a'+index)), 32))
		}
		if e != nil || scanErr != nil || member.Member.ID != id || member.Member.PeerURL != fmt.Sprintf("quic://10.0.0.%d:9090", index) || member.Member.PublicKey != key {
			t.Fatalf("bad member: %+v", member)
		}
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any, map[string]any)
	}{
		{"deleting pod", func(a, b map[string]any) {
			a["metadata"].(map[string]any)["deletionTimestamp"] = "2026-01-01T00:00:00Z"
		}},
		{"duplicate pod uid", func(a, b map[string]any) {
			b["metadata"].(map[string]any)["uid"] = a["metadata"].(map[string]any)["uid"]
		}},
		{"duplicate endpoint", func(a, b map[string]any) {
			b["status"].(map[string]any)["podIP"] = a["status"].(map[string]any)["podIP"]
		}},
		{"duplicate key", func(a, b map[string]any) {
			first := a["status"].(map[string]any)["initContainerStatuses"].([]any)[0].(map[string]any)["state"].(map[string]any)
			second := b["status"].(map[string]any)["initContainerStatuses"].([]any)[0].(map[string]any)["state"].(map[string]any)
			second["terminated"] = first["terminated"]
		}},
		{"owner mismatch", func(a, b map[string]any) {
			a["metadata"].(map[string]any)["ownerReferences"].([]any)[0].(map[string]any)["uid"] = "foreign"
		}},
		{"last state only", func(a, b map[string]any) {
			s := a["status"].(map[string]any)["initContainerStatuses"].([]any)[0].(map[string]any)
			s["lastState"] = s["state"]
			s["state"] = map[string]any{"waiting": map[string]any{}}
		}},
		{"main previously started", func(a, b map[string]any) {
			a["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["lastState"] = map[string]any{"terminated": map[string]any{"exitCode": 0}}
		}},
		{"privileged main", func(a, b map[string]any) {
			a["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["securityContext"] = map[string]any{"privileged": true}
		}},
		{"extra main mount", func(a, b map[string]any) {
			c := a["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
			c["volumeMounts"] = append(c["volumeMounts"].([]any), map[string]any{"name": "kube-api-access-abc", "mountPath": "/var/lib/rhiza/identity.json", "readOnly": true})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := pods
			data, _ := json.Marshal(original)
			var changed []any
			if err := json.Unmarshal(data, &changed); err != nil {
				t.Fatal(err)
			}
			tc.mutate(changed[0].(map[string]any), changed[1].(map[string]any))
			pods = changed
			defer func() { pods = original }()
			if _, err := check(); err == nil {
				t.Fatal("invalid cohort accepted")
			}
		})
	}
	validSources := projected["sources"]
	projected["sources"] = []any{map[string]any{"secret": map[string]any{"name": "other"}}}
	if _, err := check(); err == nil {
		t.Fatal("accepted arbitrary secret projection")
	}
	projected["sources"] = []any{map[string]any{"hostPath": map[string]any{"path": "/var/lib/rhiza"}}}
	if _, err := check(); err == nil {
		t.Fatal("accepted shadowing host path")
	}
	projected["sources"] = validSources
	init[0]["volumeMounts"] = append(init[0]["volumeMounts"].([]any), map[string]any{"name": "kube-api-access-abc", "mountPath": "/var/lib/rhiza/identity.json", "readOnly": true})
	if _, err := check(); err == nil {
		t.Fatal("accepted identity path shadow")
	}
}

func TestBootstrapInitTemplateDefaults(t *testing.T) {
	init, err := V1Alpha2BootstrapInitContainers("example/app:v1", "uid")
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := json.Marshal(init)
	for _, c := range init {
		c["imagePullPolicy"] = "IfNotPresent"
		c["resources"] = map[string]any{}
		c["volumeMounts"] = append(c["volumeMounts"].([]any), map[string]any{"name": "kube-api-access-abc", "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": true})
	}
	actual, _ := json.Marshal(init)
	if !bootstrapInitMatches(actual, expected) {
		t.Fatal("rejected Kubernetes defaults")
	}
	init[0]["imagePullPolicy"] = "Always"
	actual, _ = json.Marshal(init)
	if bootstrapInitMatches(actual, expected) {
		t.Fatal("accepted non-default pull policy")
	}
}

func TestDecodeV1Alpha2BootstrapReport(t *testing.T) {
	key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
	if _, err := DecodeV1Alpha2BootstrapReport(`{"version":1,"publicKey":"` + key + `"}`); err != nil {
		t.Fatal(err)
	}
	for _, input := range []string{"", `{"version":2,"publicKey":"` + key + `"}`, `{"version":1,"publicKey":"` + key + `","secret":"x"}`, `{"version":1,"publicKey":"bad"}`, `{"version":1,"version":1,"publicKey":"` + key + `"}`, `{"version":1,"publicKey":"` + key + `"} {}`, strings.Repeat("x", 513)} {
		if _, err := DecodeV1Alpha2BootstrapReport(input); err == nil {
			t.Fatalf("accepted report %q", input)
		}
	}
}
