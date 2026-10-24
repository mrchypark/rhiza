package operator

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestStaticVoterStartupGrantUsesOnlyCurrentAuthenticatedVoterCredentials(t *testing.T) {
	const (
		podName = "rhiza-0"
		podUID  = "pod-uid-0"
		rootUID = "root-uid"
		stsUID  = "sts-uid"
	)
	peerToken := "private-token-rhiza-0"
	members, err := json.Marshal([]quepaxa.Member{{
		ID: quepaxa.NodeID(podName), URL: "http://rhiza-0:8080", PeerURL: "quic://rhiza-0:9090",
		PublicKey: quepaxa.PublicKey(network.PeerPublicKey(types.ClusterID("source"), quepaxa.NodeID(podName), peerToken)),
	}})
	if err != nil {
		t.Fatal(err)
	}
	peerTokens, _ := json.Marshal(map[string]string{podName: peerToken, "rhiza-1": "other-private-token"})

	newFixture := func(t *testing.T) (*FencingExecutor, object, object, object, object) {
		t.Helper()
		template := object{
			"metadata": object{"annotations": object{fencingProtocolAnnotation: "1", fencingRootAnnotation: rootUID}},
			"spec": object{"containers": []any{object{
				"name": "rhiza", "command": []any{"/usr/local/bin/rhiza-fencer"}, "args": []any{"supervise", "--", "/usr/local/bin/rhiza"},
				"securityContext": object{"allowPrivilegeEscalation": false, "capabilities": object{"drop": []any{"ALL"}}},
				"envFrom":         []any{object{"configMapRef": object{"name": "rhiza-config"}}, object{"secretRef": object{"name": "peer-credentials"}}},
			}}},
		}
		// A live Pod and its StatefulSet template are separate API objects. Keep
		// their nested specs independent so Pod-only drift cannot also mutate the
		// template used as the authority for the grant.
		podSpecJSON, err := json.Marshal(template["spec"])
		if err != nil {
			t.Fatal(err)
		}
		var podSpec object
		if err := json.Unmarshal(podSpecJSON, &podSpec); err != nil {
			t.Fatal(err)
		}
		pod := object{
			"metadata": object{"name": podName, "uid": podUID, "annotations": object{fencingProtocolAnnotation: "1", fencingRootAnnotation: rootUID},
				"ownerReferences": []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": stsUID, "controller": true}}},
			"spec": podSpec,
		}
		sts := object{"metadata": object{"name": "rhiza", "uid": stsUID}, "spec": object{"template": template}}
		root := object{"apiVersion": "rhiza.mrchypark.dev/v1alpha1", "kind": "RhizaCluster", "metadata": object{"name": "rhiza", "uid": rootUID}, "spec": object{
			"statefulSet": "rhiza", "container": "rhiza", "adoptClusterID": "source", "automatic": false,
		}}
		secret := object{"metadata": object{"name": "peer-credentials", "uid": "secret-uid"}, "data": object{"RHIZA_PEER_TOKENS": base64.StdEncoding.EncodeToString(peerTokens)}}
		config := object{"data": object{"RHIZA_CLUSTER_ID": "source", "RHIZA_CLUSTER_MEMBERS": string(members)}}
		api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/api/v1/namespaces/default/pods/" + podName:
				_ = json.NewEncoder(w).Encode(pod)
			case "/api/v1/namespaces/default/configmaps/rhiza-config":
				_ = json.NewEncoder(w).Encode(config)
			case "/api/v1/namespaces/default/secrets/peer-credentials":
				_ = json.NewEncoder(w).Encode(secret)
			case "/apis/apps/v1/namespaces/default/statefulsets/rhiza":
				_ = json.NewEncoder(w).Encode(sts)
			case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizaclusters":
				_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
			default:
				http.NotFound(w, r)
			}
		}))
		t.Cleanup(api.Close)
		tokenFile := filepath.Join(t.TempDir(), "service-account-token")
		if err := os.WriteFile(tokenFile, []byte("test-token"), 0600); err != nil {
			t.Fatal(err)
		}
		kube := &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenFile, Client: api.Client()}
		return &FencingExecutor{Kube: kube, LegacyCooperativeControlState: true}, pod, root, secret, sts
	}

	t.Run("valid static cooperative root needs no automatic control record", func(t *testing.T) {
		e, _, _, _, _ := newFixture(t)
		request := fencing.Identity{SourceClusterID: "source", NodeID: podName, PodName: podName, PodUID: podUID, RunID: "run-1"}
		grant, err := e.resolveStartupGrant(context.Background(), stsUID, request)
		if err != nil {
			t.Fatal(err)
		}
		if grant.Role != fencing.StartupRoleVoterActive || grant.Identity != request {
			t.Fatalf("unexpected grant identity/role: %+v", grant)
		}
		if len(grant.Environment) != 2 || grant.Environment["RHIZA_CLUSTER_MEMBERS"] == "" || grant.Environment["RHIZA_PEER_TOKEN"] != peerToken {
			t.Fatalf("grant did not contain only membership and own token: %#v", grant.Environment)
		}
		for _, forbidden := range []string{"RHIZA_ADMIN_TOKEN", "RHIZA_PEER_TOKENS", "RHIZA_NODE_ID"} {
			if _, ok := grant.Environment[forbidden]; ok {
				t.Fatalf("grant leaked or overrode %s", forbidden)
			}
		}
		if err := grant.Validate(request); err != nil {
			t.Fatalf("static grant failed protocol validation: %v", err)
		}
	})
	t.Run("legacy cooperative root needs no RhizaCluster CRD", func(t *testing.T) {
		e, pod, root, _, sts := newFixture(t)
		delete(asObject(nested(pod, "metadata", "annotations")), fencingRootAnnotation)
		delete(asObject(nested(sts, "spec", "template", "metadata", "annotations")), fencingRootAnnotation)
		// No RhizaCluster UID is available in this pre-existing cooperative
		// profile. initializeRoot has already authorized the registered source.
		delete(asObject(root["metadata"]), "uid")
		request := fencing.Identity{SourceClusterID: "source", NodeID: podName, PodName: podName, PodUID: podUID, RunID: "run-1"}
		grant, err := e.resolveStartupGrant(context.Background(), stsUID, request)
		if err != nil || grant.Environment["RHIZA_PEER_TOKEN"] != peerToken {
			t.Fatalf("legacy cooperative static grant=%+v err=%v", grant, err)
		}
	})
	t.Run("SecretKeyRef peer-token map remains compatible", func(t *testing.T) {
		e, pod, _, _, sts := newFixture(t)
		removeStaticStartupSecretEnvFrom(pod, sts, "peer-credentials")
		setStaticStartupEnvOnPodAndTemplate(t, pod, sts, object{
			"name":      "RHIZA_PEER_TOKENS",
			"valueFrom": object{"secretKeyRef": object{"name": "peer-credentials", "key": "RHIZA_PEER_TOKENS"}},
		})
		request := fencing.Identity{SourceClusterID: "source", NodeID: podName, PodName: podName, PodUID: podUID, RunID: "run-1"}
		grant, err := e.resolveStartupGrant(context.Background(), stsUID, request)
		if err != nil || grant.Environment["RHIZA_PEER_TOKEN"] != peerToken {
			t.Fatalf("SecretKeyRef map grant=%+v err=%v", grant, err)
		}
	})
	t.Run("SecretKeyRef direct peer token remains compatible", func(t *testing.T) {
		e, pod, _, secret, sts := newFixture(t)
		removeStaticStartupSecretEnvFrom(pod, sts, "peer-credentials")
		secretData := asObject(secret["data"])
		delete(secretData, "RHIZA_PEER_TOKENS")
		secretData["RHIZA_PEER_TOKEN"] = base64.StdEncoding.EncodeToString([]byte(peerToken))
		setStaticStartupEnvOnPodAndTemplate(t, pod, sts, object{
			"name":      "RHIZA_PEER_TOKEN",
			"valueFrom": object{"secretKeyRef": object{"name": "peer-credentials", "key": "RHIZA_PEER_TOKEN"}},
		})
		request := fencing.Identity{SourceClusterID: "source", NodeID: podName, PodName: podName, PodUID: podUID, RunID: "run-1"}
		grant, err := e.resolveStartupGrant(context.Background(), stsUID, request)
		if err != nil || grant.Environment["RHIZA_PEER_TOKEN"] != peerToken {
			t.Fatalf("SecretKeyRef direct-token grant=%+v err=%v", grant, err)
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, object, object, object, object)
	}{
		{name: "wrong authenticated Pod UID", mutate: func(_ *testing.T, pod, _, _, _ object) { asObject(pod["metadata"])["uid"] = "other-pod" }},
		{name: "wrong StatefulSet owner", mutate: func(_ *testing.T, pod, _, _, _ object) {
			asObject(list(nested(pod, "metadata", "ownerReferences"))[0])["uid"] = "other-sts"
		}},
		{name: "wrong root UID", mutate: func(_ *testing.T, pod, root, _, _ object) {
			asObject(nested(pod, "metadata", "annotations"))[fencingRootAnnotation] = "other-root"
			asObject(root["metadata"])["uid"] = "other-root"
		}},
		{name: "Pod-only cluster ID drift", mutate: func(_ *testing.T, pod, _, _, _ object) {
			setStaticStartupPodEnv(pod, "RHIZA_CLUSTER_ID", "other-source")
		}},
		{name: "Pod-only membership drift", mutate: func(_ *testing.T, pod, _, _, _ object) {
			setStaticStartupPodEnv(pod, "RHIZA_CLUSTER_MEMBERS", `[]`)
		}},
		{name: "Pod-only object-store bucket drift", mutate: func(_ *testing.T, pod, _, _, _ object) {
			setStaticStartupPodEnv(pod, "RHIZA_OBJSTORE_BUCKET", "other-bucket")
		}},
		{name: "Pod-only object-store prefix drift", mutate: func(_ *testing.T, pod, _, _, _ object) {
			setStaticStartupPodEnv(pod, "RHIZA_OBJSTORE_PREFIX", "other-prefix")
		}},
		{name: "Pod-only token-map drift", mutate: func(_ *testing.T, pod, _, _, _ object) {
			setStaticStartupPodEnv(pod, "RHIZA_PEER_TOKENS", `{"rhiza-0":"different-token"}`)
		}},
		{name: "peer token map and scalar conflict", mutate: func(t *testing.T, pod, _, secret, sts object) {
			asObject(secret["data"])["RHIZA_PEER_TOKEN"] = base64.StdEncoding.EncodeToString([]byte(peerToken))
			setStaticStartupEnvOnPodAndTemplate(t, pod, sts, object{
				"name":      "RHIZA_PEER_TOKEN",
				"valueFrom": object{"secretKeyRef": object{"name": "peer-credentials", "key": "RHIZA_PEER_TOKEN"}},
			})
		}},
		{name: "ConfigMap-backed token map rejected", mutate: func(t *testing.T, pod, _, _, sts object) {
			setStaticStartupEnvOnPodAndTemplate(t, pod, sts, object{
				"name":      "RHIZA_PEER_TOKENS",
				"valueFrom": object{"configMapKeyRef": object{"name": "rhiza-config", "key": "RHIZA_CLUSTER_MEMBERS"}},
			})
		}},
		{name: "literal token map rejected", mutate: func(t *testing.T, pod, _, _, sts object) {
			setStaticStartupEnvOnPodAndTemplate(t, pod, sts, object{"name": "RHIZA_PEER_TOKENS", "value": string(peerTokens)})
		}},
		{name: "missing own token", mutate: func(_ *testing.T, _, _, secret, _ object) {
			asObject(secret["data"])["RHIZA_PEER_TOKENS"] = base64.StdEncoding.EncodeToString([]byte(`{"rhiza-1":"other-token"}`))
		}},
		{name: "token public key mismatch", mutate: func(_ *testing.T, _, _, secret, _ object) {
			asObject(secret["data"])["RHIZA_PEER_TOKENS"] = base64.StdEncoding.EncodeToString([]byte(`{"rhiza-0":"wrong-token"}`))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, pod, root, secret, sts := newFixture(t)
			tc.mutate(t, pod, root, secret, sts)
			request := fencing.Identity{SourceClusterID: "source", NodeID: podName, PodName: podName, PodUID: podUID, RunID: "run-1"}
			if grant, err := e.resolveStartupGrant(context.Background(), stsUID, request); err == nil {
				t.Fatalf("invalid static startup binding granted role=%q env=%#v", grant.Role, grant.Environment)
			}
		})
	}
}

func removeStaticStartupSecretEnvFrom(pod, sts object, secretName string) {
	for _, container := range []object{
		asObject(list(nested(pod, "spec", "containers"))[0]),
		asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0]),
	} {
		var retained []any
		for _, item := range list(container["envFrom"]) {
			if str(nested(asObject(item), "secretRef", "name")) != secretName {
				retained = append(retained, item)
			}
		}
		container["envFrom"] = retained
	}
}

func setStaticStartupEnvOnPodAndTemplate(t *testing.T, pod, sts object, env object) {
	t.Helper()
	for _, container := range []object{
		asObject(list(nested(pod, "spec", "containers"))[0]),
		asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0]),
	} {
		data, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		var entry object
		if err := json.Unmarshal(data, &entry); err != nil {
			t.Fatal(err)
		}
		container["env"] = []any{entry}
	}
}

func setStaticStartupPodEnv(pod object, name, value string) {
	container := asObject(list(nested(pod, "spec", "containers"))[0])
	container["env"] = []any{object{"name": name, "value": value}}
}
