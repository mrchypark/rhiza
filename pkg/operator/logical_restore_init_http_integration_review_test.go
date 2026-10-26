package operator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/thanos-io/objstore"
	"gopkg.in/yaml.v3"
)

const (
	logicalManifestNamespace = "rhiza"
	logicalManifestPodName   = "rhiza-0"
	logicalManifestPodUID    = "logical-manifest-pod-uid"
	logicalManifestSTSUID    = "logical-manifest-sts-uid"
	logicalManifestRootUID   = "logical-manifest-root-uid"
	logicalManifestSourceID  = "logical-manifest-source"
	logicalManifestPodToken  = "logical-manifest-pod-token"
	logicalManifestKubeToken = "logical-manifest-kube-token"
)

type logicalManifestHTTPFixture struct {
	e            *FencingExecutor
	state        *AuthorityStateStore
	bucket       objstore.Bucket
	server       *httptest.Server
	grant        fencing.StartupGrant
	identity     fencing.Identity
	config       map[string]any
	secret       map[string]any
	sts          object
	pod          object
	tokenReviews *atomic.Int32
	podTokenFile string
}

func newLogicalManifestHTTPFixture(t *testing.T, mutate func(pod, sts object)) *logicalManifestHTTPFixture {
	t.Helper()
	sts, err := renderedLogicalAuthorityStatefulSet(t)
	if err != nil {
		t.Fatal(err)
	}
	template := asObject(nested(sts, "spec", "template"))
	metadata := asObject(template["metadata"])
	if metadata == nil {
		metadata = object{}
		template["metadata"] = metadata
	}
	metadata["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: logicalManifestRootUID}
	stsMetadata := asObject(sts["metadata"])
	stsMetadata["name"], stsMetadata["namespace"], stsMetadata["uid"] = "rhiza", logicalManifestNamespace, logicalManifestSTSUID

	podSpec := cloneObject(t, asObject(template["spec"]))
	pod := object{"apiVersion": "v1", "kind": "Pod", "metadata": object{
		"name": logicalManifestPodName, "namespace": logicalManifestNamespace, "uid": logicalManifestPodUID,
		"annotations":     object{fencingProtocolAnnotation: "1", fencingRootAnnotation: logicalManifestRootUID},
		"ownerReferences": []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": logicalManifestSTSUID, "controller": true}},
	}, "spec": podSpec}
	if mutate != nil {
		mutate(pod, sts)
	}

	peerToken := "manifest-peer-token"
	var memberKey quepaxa.PublicKey
	copy(memberKey[:], network.PeerPublicKey(typesClusterID(logicalManifestSourceID), quepaxa.NodeID(logicalManifestPodName), peerToken))
	members, err := json.Marshal([]quepaxa.Member{{ID: quepaxa.NodeID(logicalManifestPodName), PublicKey: memberKey}})
	if err != nil {
		t.Fatal(err)
	}
	peerTokens, err := json.Marshal(map[string]string{logicalManifestPodName: peerToken})
	if err != nil {
		t.Fatal(err)
	}
	config := object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": "rhiza-config", "namespace": logicalManifestNamespace}, "data": object{
		"RHIZA_CLUSTER_ID": logicalManifestSourceID,
	}}
	objectStore := object{"apiVersion": "v1", "kind": "ConfigMap", "metadata": object{"name": "rhiza-object-store", "namespace": logicalManifestNamespace}, "data": object{
		"RHIZA_OBJSTORE_PROVIDER": "gcs", "RHIZA_OBJSTORE_BUCKET": "logical-manifest-archive", "RHIZA_OBJSTORE_PREFIX": "archive/" + logicalManifestSourceID,
	}}
	secret := object{"apiVersion": "v1", "kind": "Secret", "metadata": object{"name": "rhiza-peer-credentials", "namespace": logicalManifestNamespace}, "data": object{
		"members":           base64.StdEncoding.EncodeToString(members),
		"RHIZA_PEER_TOKENS": base64.StdEncoding.EncodeToString(peerTokens),
	}}
	root := ClusterResource{Metadata: object{"name": "rhiza", "namespace": logicalManifestNamespace, "uid": logicalManifestRootUID}, Spec: ClusterSpec{
		LogicalID: "logical-manifest", StatefulSet: "rhiza", Container: "rhiza", AdoptClusterID: logicalManifestSourceID,
	}}

	tokenReviews := &atomic.Int32{}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer "+logicalManifestKubeToken {
			http.Error(w, "wrong operator token", http.StatusUnauthorized)
			return
		}
		switch r.Method + " " + r.URL.Path {
		case "POST /apis/authentication.k8s.io/v1/tokenreviews":
			tokenReviews.Add(1)
			var request struct {
				Spec struct {
					Token string `json:"token"`
				} `json:"spec"`
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, "bad TokenReview", http.StatusBadRequest)
				return
			}
			authenticated := request.Spec.Token == logicalManifestPodToken
			status := object{"authenticated": authenticated}
			if authenticated {
				status["audiences"] = []any{"rhiza-fencer"}
				status["user"] = object{"username": "system:serviceaccount:" + logicalManifestNamespace + ":rhiza-voter-reader", "extra": object{
					"authentication.kubernetes.io/pod-uid":  []any{logicalManifestPodUID},
					"authentication.kubernetes.io/pod-name": []any{logicalManifestPodName},
				}}
			}
			_ = json.NewEncoder(w).Encode(object{"status": status})
		case "GET /api/v1/namespaces/" + logicalManifestNamespace + "/pods/" + logicalManifestPodName:
			_ = json.NewEncoder(w).Encode(pod)
		case "GET /api/v1/namespaces/" + logicalManifestNamespace + "/configmaps/rhiza-config":
			_ = json.NewEncoder(w).Encode(config)
		case "GET /api/v1/namespaces/" + logicalManifestNamespace + "/configmaps/rhiza-object-store":
			_ = json.NewEncoder(w).Encode(objectStore)
		case "GET /api/v1/namespaces/" + logicalManifestNamespace + "/secrets/rhiza-peer-credentials":
			_ = json.NewEncoder(w).Encode(secret)
		case "GET /apis/apps/v1/namespaces/" + logicalManifestNamespace + "/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(sts)
		case "GET /apis/apps/v1/namespaces/" + logicalManifestNamespace + "/statefulsets":
			_ = json.NewEncoder(w).Encode(object{"items": []any{sts}})
		case "GET /apis/rhiza.mrchypark.dev/v1alpha1/namespaces/" + logicalManifestNamespace + "/rhizaclusters":
			_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	kubeToken := filepath.Join(t.TempDir(), "operator-token")
	if err := os.WriteFile(kubeToken, []byte(logicalManifestKubeToken), 0600); err != nil {
		t.Fatal(err)
	}
	podTokenFile := filepath.Join(t.TempDir(), "pod-token")
	if err := os.WriteFile(podTokenFile, []byte(logicalManifestPodToken), 0600); err != nil {
		t.Fatal(err)
	}
	bucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = bucket.Close() })
	state, err := NewAuthorityStateStore(bucket, "logical-manifest-http")
	if err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: logicalManifestNamespace, TokenFile: kubeToken, Client: api.Client()},
		AuthorityState: state, ArchiveBucket: bucket, RequireMutationJournal: true, AuthorityProfile: fencing.LogicalWriterProfileV1}
	authority := httptest.NewTLSServer(e.Handler())
	t.Cleanup(authority.Close)
	identity := fencing.Identity{SourceClusterID: logicalManifestSourceID, NodeID: logicalManifestPodName, PodName: logicalManifestPodName,
		PodUID: logicalManifestPodUID, RunID: "logical-manifest-run"}
	message := fencing.AuthorityMessage{Action: "register", AuthorityProfile: fencing.LogicalWriterProfileV1, Identity: identity}
	body, err := json.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, authority.URL+"/v1/supervisor", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+logicalManifestPodToken)
	response, err := authority.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if mutate == nil && response.StatusCode != http.StatusOK {
		t.Fatalf("rendered manifest registration: status=%d body=%s", response.StatusCode, responseBody)
	}
	if mutate != nil && (response.StatusCode == http.StatusOK || response.StatusCode != http.StatusForbidden && response.StatusCode != http.StatusConflict) {
		t.Fatalf("altered rendered Pod was not rejected at authentication: status=%d body=%s", response.StatusCode, responseBody)
	}
	var grant fencing.StartupGrant
	if response.StatusCode == http.StatusOK {
		if err := json.Unmarshal(responseBody, &grant); err != nil {
			t.Fatal(err)
		}
	}
	return &logicalManifestHTTPFixture{e: e, state: state, bucket: bucket, server: authority, grant: grant, identity: identity,
		config: config, secret: secret, sts: sts, pod: pod, tokenReviews: tokenReviews, podTokenFile: podTokenFile}
}

func renderedLogicalAuthorityStatefulSet(t *testing.T) (object, error) {
	t.Helper()
	file, err := os.ReadFile("../../deploy/operator/logical-authority/statefulset.yaml")
	if err != nil {
		return nil, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(file))
	for {
		var document map[string]any
		if err := decoder.Decode(&document); err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("rendered logical authority manifest has no StatefulSet")
			}
			return nil, err
		}
		if document["kind"] != "StatefulSet" {
			continue
		}
		encoded, err := json.Marshal(document)
		if err != nil {
			return nil, err
		}
		var sts object
		if err := json.Unmarshal(encoded, &sts); err != nil {
			return nil, err
		}
		// This file is a deployment template; replace only its documented digest
		// placeholder with a deterministic syntactically valid test digest.
		for _, group := range []string{"containers", "initContainers"} {
			for _, raw := range list(nested(sts, "spec", "template", "spec", group)) {
				container := asObject(raw)
				container["image"] = strings.ReplaceAll(str(container["image"]), "REPLACE_WITH_64_HEX_DIGEST", strings.Repeat("a", 64))
			}
		}
		return sts, nil
	}
}

func typesClusterID(value string) types.ClusterID { return types.ClusterID(value) }

func logicalManifestHTTPPost(t *testing.T, fixture *logicalManifestHTTPFixture, route string, value any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, fixture.server.URL+route, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+logicalManifestPodToken)
	response, err := fixture.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	result := httptest.NewRecorder()
	result.WriteHeader(response.StatusCode)
	_, _ = io.Copy(result, response.Body)
	return result
}

func TestLogicalRestoreInitRenderedManifestTokenReviewAndHandlerRegistration(t *testing.T) {
	fixture := newLogicalManifestHTTPFixture(t, nil)
	if fixture.tokenReviews.Load() == 0 {
		t.Fatal("registration did not traverse Kubernetes TokenReview")
	}
	if fixture.grant.AuthorityProfile != fencing.LogicalWriterProfileV1 || fixture.grant.LogicalBinding == nil || fixture.grant.Identity != fixture.identity {
		t.Fatalf("rendered workload registration returned a nonlogical grant: %+v", fixture.grant)
	}
	root, _, err := fixture.state.ReadRoot(t.Context(), logicalManifestSTSUID, logicalManifestSourceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := root.Ledger.Runs[fixture.identity.RunID]; !ok {
		t.Fatal("actual handler did not persist the rendered Pod's registration")
	}
	init := asObject(list(nested(fixture.pod, "spec", "initContainers"))[0])
	if !logicalRestoreInitEnvFrom(init["envFrom"]) || !logicalRestoreInitMounts(init["volumeMounts"]) {
		t.Fatal("rendered restore-init does not retain reader-only credentials and staging-only mounts")
	}
	for _, mount := range list(init["volumeMounts"]) {
		name := str(asObject(mount)["name"])
		if name == "data" || name == "authority-token" || name == "authority-ca" {
			t.Fatalf("rendered init unexpectedly has writer/main-only mount %q", name)
		}
	}
}

func TestLogicalRestoreInitHandlerRejectsSecretAuthorityAndDataMounts(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(pod, sts object)
	}{
		{"peer credential Secret", func(pod, sts object) {
			for _, spec := range []object{asObject(pod["spec"]), asObject(nested(sts, "spec", "template", "spec"))} {
				init := asObject(list(spec["initContainers"])[0])
				init["envFrom"] = append(list(init["envFrom"]), object{"secretRef": object{"name": "rhiza-peer-credentials"}})
			}
		}},
		{"authority token mount", func(pod, sts object) {
			for _, spec := range []object{asObject(pod["spec"]), asObject(nested(sts, "spec", "template", "spec"))} {
				init := asObject(list(spec["initContainers"])[0])
				init["volumeMounts"] = append(list(init["volumeMounts"]), object{"name": "authority-token", "mountPath": "/var/run/secrets/rhiza-authority"})
			}
		}},
		{"main DataDir mount", func(pod, sts object) {
			for _, spec := range []object{asObject(pod["spec"]), asObject(nested(sts, "spec", "template", "spec"))} {
				init := asObject(list(spec["initContainers"])[0])
				init["volumeMounts"] = append(list(init["volumeMounts"]), object{"name": "data", "mountPath": "/data"})
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLogicalManifestHTTPFixture(t, tc.mutate)
			if fixture.tokenReviews.Load() == 0 {
				t.Fatal("malicious registration was rejected before the authenticated handler path")
			}
			root, _, err := fixture.state.ReadRoot(t.Context(), logicalManifestSTSUID, logicalManifestSourceID)
			if err != nil && !errors.Is(err, errAuthorityStateNotFound) {
				t.Fatal(err)
			}
			if root.Ledger.Runs != nil && len(root.Ledger.Runs) != 0 {
				t.Fatalf("malicious restore-init spec created an authority Run: %+v", root.Ledger.Runs)
			}
		})
	}
}
