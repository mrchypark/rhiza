package operator

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mrchypark/rhiza/internal/types"
	"gopkg.in/yaml.v3"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
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
		case strings.Contains(r.URL.Path, "/serviceaccounts/"):
			respond(managedFixtureServiceAccount(phase2Ref))
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
					managedFixtureAdmitPodToken(spec)
					main := spec["containers"].([]any)[0].(map[string]any)
					switch podFault {
					case "wrong account":
						spec["serviceAccountName"] = "rhiza-operator"
					case "token disabled":
						spec["automountServiceAccountToken"] = false
					case "token missing":
						spec["volumes"] = spec["volumes"].([]any)[:2]
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
	if pod["serviceAccountName"] != V1Alpha2ManagedHostServiceAccount || pod["automountServiceAccountToken"] != true {
		t.Fatal("dedicated host account omitted")
	}
	main := pod["containers"].([]any)[0].(map[string]any)
	if !sameJSON(mustManagedJSON(main["command"]), json.RawMessage(`["/usr/local/bin/rhiza-ha-init"]`)) || !sameJSON(mustManagedJSON(main["args"]), json.RawMessage(`["serve-managed-sql","--runtime-profile","/etc/rhiza/managed/runtime.json","--identity-file","/var/lib/rhiza/identity.json","--handoff-file","/var/lib/rhiza/bootstrap.json","--listen","0.0.0.0:8080"]`)) {
		t.Fatal("main reference-host contract missing")
	}
	if !sameJSON(mustManagedJSON(main["readinessProbe"]), json.RawMessage(`{"exec":{"command":["/usr/local/bin/rhiza-ha-init","check-ready","--runtime-profile","/etc/rhiza/managed/runtime.json"]},"periodSeconds":5,"timeoutSeconds":2,"successThreshold":1,"failureThreshold":3}`)) || main["livenessProbe"] != nil {
		t.Fatal("private readiness-only probe missing")
	}
	ready = true
	for _, fault := range []string{"missing readiness", "liveness", "listen changed", "wrong account", "token disabled", "token missing"} {
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

// Captured public objects from e7da4cc bootstrap 41030761470. No Secret data.
const managedCapturedKubeletObjects = "[{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"creationTimestamp\":\"2026-10-30T09:08:59Z\",\"generateName\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75-\",\"generation\":1,\"labels\":{\"pod-template-hash\":\"7494bb4b75\",\"rhiza.mrchypark.dev/cluster-uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"rhiza.mrchypark.dev/generation\":\"0\"},\"name\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75-7k7nc\",\"namespace\":\"managed-bootstrap\",\"ownerReferences\":[{\"apiVersion\":\"apps/v1\",\"blockOwnerDeletion\":true,\"controller\":true,\"kind\":\"ReplicaSet\",\"name\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75\",\"uid\":\"4f355a34-f5e3-4c91-9170-40d60031b197\"}],\"resourceVersion\":\"929\",\"uid\":\"35985d0e-deaa-4890-ae51-652d3762a696\"},\"spec\":{\"automountServiceAccountToken\":true,\"containers\":[{\"args\":[\"serve-managed-sql\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--handoff-file\",\"/var/lib/rhiza/bootstrap.json\",\"--listen\",\"0.0.0.0:8080\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"},{\"name\":\"RHIZA_CLUSTER_UID\",\"value\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"sql\",\"readinessProbe\":{\"exec\":{\"command\":[\"/usr/local/bin/rhiza-ha-init\",\"check-ready\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\"]},\"failureThreshold\":3,\"periodSeconds\":5,\"successThreshold\":1,\"timeoutSeconds\":2},\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/etc/rhiza/managed\",\"name\":\"rhiza-runtime-profile\",\"readOnly\":true},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pjbmd\",\"readOnly\":true}]}],\"dnsPolicy\":\"ClusterFirst\",\"enableServiceLinks\":true,\"initContainers\":[{\"args\":[\"prepare-identity\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--termination-message\",\"/dev/termination-log\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-prepare-identity\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pjbmd\",\"readOnly\":true}]},{\"args\":[\"await-bootstrap-plan\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-await-bootstrap-plan\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pjbmd\",\"readOnly\":true}]}],\"nodeName\":\"ha-managed-41030761470-1-a42831e923c4-control-plane\",\"preemptionPolicy\":\"PreemptLowerPriority\",\"priority\":0,\"restartPolicy\":\"Always\",\"schedulerName\":\"default-scheduler\",\"securityContext\":{\"fsGroup\":65532,\"runAsGroup\":65532,\"runAsNonRoot\":true,\"runAsUser\":65532},\"serviceAccount\":\"rhiza-managed-host\",\"serviceAccountName\":\"rhiza-managed-host\",\"terminationGracePeriodSeconds\":30,\"tolerations\":[{\"effect\":\"NoExecute\",\"key\":\"node.kubernetes.io/not-ready\",\"operator\":\"Exists\",\"tolerationSeconds\":300},{\"effect\":\"NoExecute\",\"key\":\"node.kubernetes.io/unreachable\",\"operator\":\"Exists\",\"tolerationSeconds\":300}],\"volumes\":[{\"emptyDir\":{},\"name\":\"rhiza-data\"},{\"name\":\"rhiza-runtime-profile\",\"secret\":{\"defaultMode\":288,\"items\":[{\"key\":\"runtime.json\",\"path\":\"runtime.json\"}],\"secretName\":\"runtime\"}},{\"name\":\"kube-api-access-pjbmd\",\"projected\":{\"defaultMode\":420,\"sources\":[{\"serviceAccountToken\":{\"expirationSeconds\":3607,\"path\":\"token\"}},{\"configMap\":{\"items\":[{\"key\":\"ca.crt\",\"path\":\"ca.crt\"}],\"name\":\"kube-root-ca.crt\"}},{\"downwardAPI\":{\"items\":[{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"},\"path\":\"namespace\"}]}}]}}]},\"status\":{\"conditions\":[{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:09:00Z\",\"observedGeneration\":1,\"status\":\"True\",\"type\":\"PodReadyToStartContainers\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"message\":\"containers with incomplete status: [rhiza-await-bootstrap-plan]\",\"observedGeneration\":1,\"reason\":\"ContainersNotInitialized\",\"status\":\"False\",\"type\":\"Initialized\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"message\":\"containers with unready status: [sql]\",\"observedGeneration\":1,\"reason\":\"ContainersNotReady\",\"status\":\"False\",\"type\":\"Ready\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"message\":\"containers with unready status: [sql]\",\"observedGeneration\":1,\"reason\":\"ContainersNotReady\",\"status\":\"False\",\"type\":\"ContainersReady\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"observedGeneration\":1,\"status\":\"True\",\"type\":\"PodScheduled\"}],\"containerStatuses\":[{\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imageID\":\"\",\"lastState\":{},\"name\":\"sql\",\"ready\":false,\"restartCount\":0,\"started\":false,\"state\":{\"waiting\":{\"reason\":\"PodInitializing\"}},\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/etc/rhiza/managed\",\"name\":\"rhiza-runtime-profile\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pjbmd\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"}]}],\"hostIP\":\"172.18.0.2\",\"hostIPs\":[{\"ip\":\"172.18.0.2\"}],\"initContainerStatuses\":[{\"containerID\":\"containerd://e8d40342414efdeaa3c62168b4af87dec9b0f9a723e13a7fe697131c82100dca\",\"image\":\"docker.io/library/rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imageID\":\"docker.io/library/import-2026-10-30@sha256:02bb4edde2f979a89adca9e08c85c292601a0281181557149b99f6558b5f0cdb\",\"lastState\":{},\"name\":\"rhiza-prepare-identity\",\"ready\":true,\"resources\":{},\"restartCount\":0,\"started\":false,\"state\":{\"terminated\":{\"containerID\":\"containerd://e8d40342414efdeaa3c62168b4af87dec9b0f9a723e13a7fe697131c82100dca\",\"exitCode\":0,\"finishedAt\":\"2026-10-30T09:09:00Z\",\"message\":\"{\\\"version\\\":1,\\\"publicKey\\\":\\\"sUnaxAUlyDGg80VARSvz+AJapCwCvT8oYMxZ3QtEXtQ=\\\"}\",\"reason\":\"Completed\",\"startedAt\":\"2026-10-30T09:09:00Z\"}},\"user\":{\"linux\":{\"gid\":65532,\"supplementalGroups\":[65532],\"uid\":65532}},\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pjbmd\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"}]},{\"containerID\":\"containerd://dede1eb207d920d58a15fb75dcb8b58eda1a64c58056628993d3a184cd5e115e\",\"image\":\"docker.io/library/rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imageID\":\"docker.io/library/import-2026-10-30@sha256:02bb4edde2f979a89adca9e08c85c292601a0281181557149b99f6558b5f0cdb\",\"lastState\":{},\"name\":\"rhiza-await-bootstrap-plan\",\"ready\":false,\"resources\":{},\"restartCount\":0,\"started\":true,\"state\":{\"running\":{\"startedAt\":\"2026-10-30T09:09:00Z\"}},\"user\":{\"linux\":{\"gid\":65532,\"supplementalGroups\":[65532],\"uid\":65532}},\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pjbmd\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"}]}],\"observedGeneration\":1,\"phase\":\"Pending\",\"podIP\":\"10.244.0.8\",\"podIPs\":[{\"ip\":\"10.244.0.8\"}],\"qosClass\":\"BestEffort\",\"resources\":{},\"startTime\":\"2026-10-30T09:08:59Z\"}},{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"creationTimestamp\":\"2026-10-30T09:08:59Z\",\"generateName\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75-\",\"generation\":1,\"labels\":{\"pod-template-hash\":\"7494bb4b75\",\"rhiza.mrchypark.dev/cluster-uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"rhiza.mrchypark.dev/generation\":\"0\"},\"name\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75-9b5qm\",\"namespace\":\"managed-bootstrap\",\"ownerReferences\":[{\"apiVersion\":\"apps/v1\",\"blockOwnerDeletion\":true,\"controller\":true,\"kind\":\"ReplicaSet\",\"name\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75\",\"uid\":\"4f355a34-f5e3-4c91-9170-40d60031b197\"}],\"resourceVersion\":\"928\",\"uid\":\"604ecdc1-1466-40b4-8d0f-71740eaf1a21\"},\"spec\":{\"automountServiceAccountToken\":true,\"containers\":[{\"args\":[\"serve-managed-sql\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--handoff-file\",\"/var/lib/rhiza/bootstrap.json\",\"--listen\",\"0.0.0.0:8080\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"},{\"name\":\"RHIZA_CLUSTER_UID\",\"value\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"sql\",\"readinessProbe\":{\"exec\":{\"command\":[\"/usr/local/bin/rhiza-ha-init\",\"check-ready\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\"]},\"failureThreshold\":3,\"periodSeconds\":5,\"successThreshold\":1,\"timeoutSeconds\":2},\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/etc/rhiza/managed\",\"name\":\"rhiza-runtime-profile\",\"readOnly\":true},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pd25n\",\"readOnly\":true}]}],\"dnsPolicy\":\"ClusterFirst\",\"enableServiceLinks\":true,\"initContainers\":[{\"args\":[\"prepare-identity\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--termination-message\",\"/dev/termination-log\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-prepare-identity\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pd25n\",\"readOnly\":true}]},{\"args\":[\"await-bootstrap-plan\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-await-bootstrap-plan\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pd25n\",\"readOnly\":true}]}],\"nodeName\":\"ha-managed-41030761470-1-a42831e923c4-control-plane\",\"preemptionPolicy\":\"PreemptLowerPriority\",\"priority\":0,\"restartPolicy\":\"Always\",\"schedulerName\":\"default-scheduler\",\"securityContext\":{\"fsGroup\":65532,\"runAsGroup\":65532,\"runAsNonRoot\":true,\"runAsUser\":65532},\"serviceAccount\":\"rhiza-managed-host\",\"serviceAccountName\":\"rhiza-managed-host\",\"terminationGracePeriodSeconds\":30,\"tolerations\":[{\"effect\":\"NoExecute\",\"key\":\"node.kubernetes.io/not-ready\",\"operator\":\"Exists\",\"tolerationSeconds\":300},{\"effect\":\"NoExecute\",\"key\":\"node.kubernetes.io/unreachable\",\"operator\":\"Exists\",\"tolerationSeconds\":300}],\"volumes\":[{\"emptyDir\":{},\"name\":\"rhiza-data\"},{\"name\":\"rhiza-runtime-profile\",\"secret\":{\"defaultMode\":288,\"items\":[{\"key\":\"runtime.json\",\"path\":\"runtime.json\"}],\"secretName\":\"runtime\"}},{\"name\":\"kube-api-access-pd25n\",\"projected\":{\"defaultMode\":420,\"sources\":[{\"serviceAccountToken\":{\"expirationSeconds\":3607,\"path\":\"token\"}},{\"configMap\":{\"items\":[{\"key\":\"ca.crt\",\"path\":\"ca.crt\"}],\"name\":\"kube-root-ca.crt\"}},{\"downwardAPI\":{\"items\":[{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"},\"path\":\"namespace\"}]}}]}}]},\"status\":{\"conditions\":[{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:09:00Z\",\"observedGeneration\":1,\"status\":\"True\",\"type\":\"PodReadyToStartContainers\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"message\":\"containers with incomplete status: [rhiza-await-bootstrap-plan]\",\"observedGeneration\":1,\"reason\":\"ContainersNotInitialized\",\"status\":\"False\",\"type\":\"Initialized\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"message\":\"containers with unready status: [sql]\",\"observedGeneration\":1,\"reason\":\"ContainersNotReady\",\"status\":\"False\",\"type\":\"Ready\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"message\":\"containers with unready status: [sql]\",\"observedGeneration\":1,\"reason\":\"ContainersNotReady\",\"status\":\"False\",\"type\":\"ContainersReady\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"observedGeneration\":1,\"status\":\"True\",\"type\":\"PodScheduled\"}],\"containerStatuses\":[{\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imageID\":\"\",\"lastState\":{},\"name\":\"sql\",\"ready\":false,\"restartCount\":0,\"started\":false,\"state\":{\"waiting\":{\"reason\":\"PodInitializing\"}},\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/etc/rhiza/managed\",\"name\":\"rhiza-runtime-profile\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pd25n\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"}]}],\"hostIP\":\"172.18.0.2\",\"hostIPs\":[{\"ip\":\"172.18.0.2\"}],\"initContainerStatuses\":[{\"containerID\":\"containerd://edbf424608f5689549a01b328526dbbf68a8877b2c2396bdd34a5a3a99f3b96d\",\"image\":\"docker.io/library/rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imageID\":\"docker.io/library/import-2026-10-30@sha256:02bb4edde2f979a89adca9e08c85c292601a0281181557149b99f6558b5f0cdb\",\"lastState\":{},\"name\":\"rhiza-prepare-identity\",\"ready\":true,\"resources\":{},\"restartCount\":0,\"started\":false,\"state\":{\"terminated\":{\"containerID\":\"containerd://edbf424608f5689549a01b328526dbbf68a8877b2c2396bdd34a5a3a99f3b96d\",\"exitCode\":0,\"finishedAt\":\"2026-10-30T09:09:00Z\",\"message\":\"{\\\"version\\\":1,\\\"publicKey\\\":\\\"msgisg9hN+X3lMVhg9yTABp4MVRwU1n3+IiJjYdR6HE=\\\"}\",\"reason\":\"Completed\",\"startedAt\":\"2026-10-30T09:09:00Z\"}},\"user\":{\"linux\":{\"gid\":65532,\"supplementalGroups\":[65532],\"uid\":65532}},\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pd25n\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"}]},{\"containerID\":\"containerd://25737c3b0f919ffbe70ccbd039f0b1eb06cba4f9aef28b6b4d19c66ed471ad90\",\"image\":\"docker.io/library/rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imageID\":\"docker.io/library/import-2026-10-30@sha256:02bb4edde2f979a89adca9e08c85c292601a0281181557149b99f6558b5f0cdb\",\"lastState\":{},\"name\":\"rhiza-await-bootstrap-plan\",\"ready\":false,\"resources\":{},\"restartCount\":0,\"started\":true,\"state\":{\"running\":{\"startedAt\":\"2026-10-30T09:09:00Z\"}},\"user\":{\"linux\":{\"gid\":65532,\"supplementalGroups\":[65532],\"uid\":65532}},\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-pd25n\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"}]}],\"observedGeneration\":1,\"phase\":\"Pending\",\"podIP\":\"10.244.0.9\",\"podIPs\":[{\"ip\":\"10.244.0.9\"}],\"qosClass\":\"BestEffort\",\"resources\":{},\"startTime\":\"2026-10-30T09:08:59Z\"}},{\"apiVersion\":\"v1\",\"kind\":\"Pod\",\"metadata\":{\"creationTimestamp\":\"2026-10-30T09:08:59Z\",\"generateName\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75-\",\"generation\":1,\"labels\":{\"pod-template-hash\":\"7494bb4b75\",\"rhiza.mrchypark.dev/cluster-uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"rhiza.mrchypark.dev/generation\":\"0\"},\"name\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75-rc9z4\",\"namespace\":\"managed-bootstrap\",\"ownerReferences\":[{\"apiVersion\":\"apps/v1\",\"blockOwnerDeletion\":true,\"controller\":true,\"kind\":\"ReplicaSet\",\"name\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75\",\"uid\":\"4f355a34-f5e3-4c91-9170-40d60031b197\"}],\"resourceVersion\":\"927\",\"uid\":\"4dbf6efb-aa07-4b5c-b2eb-ab01506c9960\"},\"spec\":{\"automountServiceAccountToken\":true,\"containers\":[{\"args\":[\"serve-managed-sql\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--handoff-file\",\"/var/lib/rhiza/bootstrap.json\",\"--listen\",\"0.0.0.0:8080\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"},{\"name\":\"RHIZA_CLUSTER_UID\",\"value\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"sql\",\"readinessProbe\":{\"exec\":{\"command\":[\"/usr/local/bin/rhiza-ha-init\",\"check-ready\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\"]},\"failureThreshold\":3,\"periodSeconds\":5,\"successThreshold\":1,\"timeoutSeconds\":2},\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/etc/rhiza/managed\",\"name\":\"rhiza-runtime-profile\",\"readOnly\":true},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-jrzjr\",\"readOnly\":true}]}],\"dnsPolicy\":\"ClusterFirst\",\"enableServiceLinks\":true,\"initContainers\":[{\"args\":[\"prepare-identity\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--termination-message\",\"/dev/termination-log\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-prepare-identity\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-jrzjr\",\"readOnly\":true}]},{\"args\":[\"await-bootstrap-plan\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-await-bootstrap-plan\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-jrzjr\",\"readOnly\":true}]}],\"nodeName\":\"ha-managed-41030761470-1-a42831e923c4-control-plane\",\"preemptionPolicy\":\"PreemptLowerPriority\",\"priority\":0,\"restartPolicy\":\"Always\",\"schedulerName\":\"default-scheduler\",\"securityContext\":{\"fsGroup\":65532,\"runAsGroup\":65532,\"runAsNonRoot\":true,\"runAsUser\":65532},\"serviceAccount\":\"rhiza-managed-host\",\"serviceAccountName\":\"rhiza-managed-host\",\"terminationGracePeriodSeconds\":30,\"tolerations\":[{\"effect\":\"NoExecute\",\"key\":\"node.kubernetes.io/not-ready\",\"operator\":\"Exists\",\"tolerationSeconds\":300},{\"effect\":\"NoExecute\",\"key\":\"node.kubernetes.io/unreachable\",\"operator\":\"Exists\",\"tolerationSeconds\":300}],\"volumes\":[{\"emptyDir\":{},\"name\":\"rhiza-data\"},{\"name\":\"rhiza-runtime-profile\",\"secret\":{\"defaultMode\":288,\"items\":[{\"key\":\"runtime.json\",\"path\":\"runtime.json\"}],\"secretName\":\"runtime\"}},{\"name\":\"kube-api-access-jrzjr\",\"projected\":{\"defaultMode\":420,\"sources\":[{\"serviceAccountToken\":{\"expirationSeconds\":3607,\"path\":\"token\"}},{\"configMap\":{\"items\":[{\"key\":\"ca.crt\",\"path\":\"ca.crt\"}],\"name\":\"kube-root-ca.crt\"}},{\"downwardAPI\":{\"items\":[{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"},\"path\":\"namespace\"}]}}]}}]},\"status\":{\"conditions\":[{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:09:00Z\",\"observedGeneration\":1,\"status\":\"True\",\"type\":\"PodReadyToStartContainers\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"message\":\"containers with incomplete status: [rhiza-await-bootstrap-plan]\",\"observedGeneration\":1,\"reason\":\"ContainersNotInitialized\",\"status\":\"False\",\"type\":\"Initialized\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"message\":\"containers with unready status: [sql]\",\"observedGeneration\":1,\"reason\":\"ContainersNotReady\",\"status\":\"False\",\"type\":\"Ready\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"message\":\"containers with unready status: [sql]\",\"observedGeneration\":1,\"reason\":\"ContainersNotReady\",\"status\":\"False\",\"type\":\"ContainersReady\"},{\"lastProbeTime\":null,\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"observedGeneration\":1,\"status\":\"True\",\"type\":\"PodScheduled\"}],\"containerStatuses\":[{\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imageID\":\"\",\"lastState\":{},\"name\":\"sql\",\"ready\":false,\"restartCount\":0,\"started\":false,\"state\":{\"waiting\":{\"reason\":\"PodInitializing\"}},\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/etc/rhiza/managed\",\"name\":\"rhiza-runtime-profile\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-jrzjr\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"}]}],\"hostIP\":\"172.18.0.2\",\"hostIPs\":[{\"ip\":\"172.18.0.2\"}],\"initContainerStatuses\":[{\"containerID\":\"containerd://49267847e3034ea9d55250c4618500bc928b05136abeb66752644052c77f9771\",\"image\":\"docker.io/library/rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imageID\":\"docker.io/library/import-2026-10-30@sha256:02bb4edde2f979a89adca9e08c85c292601a0281181557149b99f6558b5f0cdb\",\"lastState\":{},\"name\":\"rhiza-prepare-identity\",\"ready\":true,\"resources\":{},\"restartCount\":0,\"started\":false,\"state\":{\"terminated\":{\"containerID\":\"containerd://49267847e3034ea9d55250c4618500bc928b05136abeb66752644052c77f9771\",\"exitCode\":0,\"finishedAt\":\"2026-10-30T09:09:00Z\",\"message\":\"{\\\"version\\\":1,\\\"publicKey\\\":\\\"OHNHVs7av5+kdH4HWU9uIgapxh0cIK0FHkRhgYONz6g=\\\"}\",\"reason\":\"Completed\",\"startedAt\":\"2026-10-30T09:09:00Z\"}},\"user\":{\"linux\":{\"gid\":65532,\"supplementalGroups\":[65532],\"uid\":65532}},\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-jrzjr\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"}]},{\"containerID\":\"containerd://65d1a15f5a9f93e5fd8c11061685b9a1fbe62483523c969140732f885df36b7e\",\"image\":\"docker.io/library/rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imageID\":\"docker.io/library/import-2026-10-30@sha256:02bb4edde2f979a89adca9e08c85c292601a0281181557149b99f6558b5f0cdb\",\"lastState\":{},\"name\":\"rhiza-await-bootstrap-plan\",\"ready\":false,\"resources\":{},\"restartCount\":0,\"started\":true,\"state\":{\"running\":{\"startedAt\":\"2026-10-30T09:09:00Z\"}},\"user\":{\"linux\":{\"gid\":65532,\"supplementalGroups\":[65532],\"uid\":65532}},\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/var/run/secrets/kubernetes.io/serviceaccount\",\"name\":\"kube-api-access-jrzjr\",\"readOnly\":true,\"recursiveReadOnly\":\"Disabled\"}]}],\"observedGeneration\":1,\"phase\":\"Pending\",\"podIP\":\"10.244.0.10\",\"podIPs\":[{\"ip\":\"10.244.0.10\"}],\"qosClass\":\"BestEffort\",\"resources\":{},\"startTime\":\"2026-10-30T09:08:59Z\"}},{\"apiVersion\":\"apps/v1\",\"kind\":\"Deployment\",\"metadata\":{\"annotations\":{\"deployment.kubernetes.io/revision\":\"1\"},\"creationTimestamp\":\"2026-10-30T09:08:59Z\",\"generation\":1,\"labels\":{\"rhiza.mrchypark.dev/cluster-uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"rhiza.mrchypark.dev/generation\":\"0\"},\"name\":\"rhiza-68c11e4838e2baf8a2eb-g0\",\"namespace\":\"managed-bootstrap\",\"ownerReferences\":[{\"apiVersion\":\"rhiza.mrchypark.dev/v1alpha2\",\"controller\":true,\"kind\":\"RhizaCluster\",\"name\":\"sql\",\"uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\"}],\"resourceVersion\":\"896\",\"uid\":\"83d179ac-9e80-452b-8035-53bc2d49f770\"},\"spec\":{\"progressDeadlineSeconds\":600,\"replicas\":3,\"revisionHistoryLimit\":10,\"selector\":{\"matchLabels\":{\"rhiza.mrchypark.dev/cluster-uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"rhiza.mrchypark.dev/generation\":\"0\"}},\"strategy\":{\"rollingUpdate\":{\"maxSurge\":\"25%\",\"maxUnavailable\":\"25%\"},\"type\":\"RollingUpdate\"},\"template\":{\"metadata\":{\"labels\":{\"rhiza.mrchypark.dev/cluster-uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"rhiza.mrchypark.dev/generation\":\"0\"}},\"spec\":{\"automountServiceAccountToken\":true,\"containers\":[{\"args\":[\"serve-managed-sql\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--handoff-file\",\"/var/lib/rhiza/bootstrap.json\",\"--listen\",\"0.0.0.0:8080\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"},{\"name\":\"RHIZA_CLUSTER_UID\",\"value\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"sql\",\"readinessProbe\":{\"exec\":{\"command\":[\"/usr/local/bin/rhiza-ha-init\",\"check-ready\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\"]},\"failureThreshold\":3,\"periodSeconds\":5,\"successThreshold\":1,\"timeoutSeconds\":2},\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/etc/rhiza/managed\",\"name\":\"rhiza-runtime-profile\",\"readOnly\":true}]}],\"dnsPolicy\":\"ClusterFirst\",\"initContainers\":[{\"args\":[\"prepare-identity\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--termination-message\",\"/dev/termination-log\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-prepare-identity\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"}]},{\"args\":[\"await-bootstrap-plan\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-await-bootstrap-plan\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"}]}],\"restartPolicy\":\"Always\",\"schedulerName\":\"default-scheduler\",\"securityContext\":{\"fsGroup\":65532,\"runAsGroup\":65532,\"runAsNonRoot\":true,\"runAsUser\":65532},\"serviceAccount\":\"rhiza-managed-host\",\"serviceAccountName\":\"rhiza-managed-host\",\"terminationGracePeriodSeconds\":30,\"volumes\":[{\"emptyDir\":{},\"name\":\"rhiza-data\"},{\"name\":\"rhiza-runtime-profile\",\"secret\":{\"defaultMode\":288,\"items\":[{\"key\":\"runtime.json\",\"path\":\"runtime.json\"}],\"secretName\":\"runtime\"}}]}}},\"status\":{\"conditions\":[{\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"lastUpdateTime\":\"2026-10-30T09:08:59Z\",\"message\":\"Deployment does not have minimum availability.\",\"reason\":\"MinimumReplicasUnavailable\",\"status\":\"False\",\"type\":\"Available\"},{\"lastTransitionTime\":\"2026-10-30T09:08:59Z\",\"lastUpdateTime\":\"2026-10-30T09:08:59Z\",\"message\":\"ReplicaSet \\\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75\\\" is progressing.\",\"reason\":\"ReplicaSetUpdated\",\"status\":\"True\",\"type\":\"Progressing\"}],\"observedGeneration\":1,\"replicas\":3,\"terminatingReplicas\":0,\"unavailableReplicas\":3,\"updatedReplicas\":3}},{\"apiVersion\":\"apps/v1\",\"kind\":\"ReplicaSet\",\"metadata\":{\"annotations\":{\"deployment.kubernetes.io/desired-replicas\":\"3\",\"deployment.kubernetes.io/max-replicas\":\"4\",\"deployment.kubernetes.io/revision\":\"1\"},\"creationTimestamp\":\"2026-10-30T09:08:59Z\",\"generation\":1,\"labels\":{\"pod-template-hash\":\"7494bb4b75\",\"rhiza.mrchypark.dev/cluster-uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"rhiza.mrchypark.dev/generation\":\"0\"},\"name\":\"rhiza-68c11e4838e2baf8a2eb-g0-7494bb4b75\",\"namespace\":\"managed-bootstrap\",\"ownerReferences\":[{\"apiVersion\":\"apps/v1\",\"blockOwnerDeletion\":true,\"controller\":true,\"kind\":\"Deployment\",\"name\":\"rhiza-68c11e4838e2baf8a2eb-g0\",\"uid\":\"83d179ac-9e80-452b-8035-53bc2d49f770\"}],\"resourceVersion\":\"894\",\"uid\":\"4f355a34-f5e3-4c91-9170-40d60031b197\"},\"spec\":{\"replicas\":3,\"selector\":{\"matchLabels\":{\"pod-template-hash\":\"7494bb4b75\",\"rhiza.mrchypark.dev/cluster-uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"rhiza.mrchypark.dev/generation\":\"0\"}},\"template\":{\"metadata\":{\"labels\":{\"pod-template-hash\":\"7494bb4b75\",\"rhiza.mrchypark.dev/cluster-uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"rhiza.mrchypark.dev/generation\":\"0\"}},\"spec\":{\"automountServiceAccountToken\":true,\"containers\":[{\"args\":[\"serve-managed-sql\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--handoff-file\",\"/var/lib/rhiza/bootstrap.json\",\"--listen\",\"0.0.0.0:8080\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"},{\"name\":\"RHIZA_CLUSTER_UID\",\"value\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"sql\",\"readinessProbe\":{\"exec\":{\"command\":[\"/usr/local/bin/rhiza-ha-init\",\"check-ready\",\"--runtime-profile\",\"/etc/rhiza/managed/runtime.json\"]},\"failureThreshold\":3,\"periodSeconds\":5,\"successThreshold\":1,\"timeoutSeconds\":2},\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"},{\"mountPath\":\"/etc/rhiza/managed\",\"name\":\"rhiza-runtime-profile\",\"readOnly\":true}]}],\"dnsPolicy\":\"ClusterFirst\",\"initContainers\":[{\"args\":[\"prepare-identity\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\",\"--termination-message\",\"/dev/termination-log\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-prepare-identity\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"}]},{\"args\":[\"await-bootstrap-plan\",\"--cr-uid\",\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\",\"--generation\",\"0\",\"--pod-uid\",\"$(POD_UID)\",\"--identity-file\",\"/var/lib/rhiza/identity.json\"],\"command\":[\"/usr/local/bin/rhiza-ha-init\"],\"env\":[{\"name\":\"POD_UID\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.uid\"}}},{\"name\":\"POD_NAME\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.name\"}}},{\"name\":\"POD_NAMESPACE\",\"valueFrom\":{\"fieldRef\":{\"apiVersion\":\"v1\",\"fieldPath\":\"metadata.namespace\"}}},{\"name\":\"RHIZA_CLUSTER_NAME\",\"value\":\"sql\"}],\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"imagePullPolicy\":\"IfNotPresent\",\"name\":\"rhiza-await-bootstrap-plan\",\"resources\":{},\"securityContext\":{\"allowPrivilegeEscalation\":false,\"capabilities\":{\"drop\":[\"ALL\"]}},\"terminationMessagePath\":\"/dev/termination-log\",\"terminationMessagePolicy\":\"File\",\"volumeMounts\":[{\"mountPath\":\"/var/lib/rhiza\",\"name\":\"rhiza-data\"}]}],\"restartPolicy\":\"Always\",\"schedulerName\":\"default-scheduler\",\"securityContext\":{\"fsGroup\":65532,\"runAsGroup\":65532,\"runAsNonRoot\":true,\"runAsUser\":65532},\"serviceAccount\":\"rhiza-managed-host\",\"serviceAccountName\":\"rhiza-managed-host\",\"terminationGracePeriodSeconds\":30,\"volumes\":[{\"emptyDir\":{},\"name\":\"rhiza-data\"},{\"name\":\"rhiza-runtime-profile\",\"secret\":{\"defaultMode\":288,\"items\":[{\"key\":\"runtime.json\",\"path\":\"runtime.json\"}],\"secretName\":\"runtime\"}}]}}},\"status\":{\"fullyLabeledReplicas\":3,\"observedGeneration\":1,\"replicas\":3,\"terminatingReplicas\":0}},{\"apiVersion\":\"rhiza.mrchypark.dev/v1alpha2\",\"kind\":\"RhizaCluster\",\"metadata\":{\"annotations\":{\"kubectl.kubernetes.io/last-applied-configuration\":\"{\\\"apiVersion\\\":\\\"rhiza.mrchypark.dev/v1alpha2\\\",\\\"kind\\\":\\\"RhizaCluster\\\",\\\"metadata\\\":{\\\"annotations\\\":{},\\\"name\\\":\\\"sql\\\",\\\"namespace\\\":\\\"managed-bootstrap\\\"},\\\"spec\\\":{\\\"genesisIntent\\\":\\\"Create\\\",\\\"logicalID\\\":\\\"managed-ci\\\",\\\"voterCount\\\":3,\\\"workload\\\":{\\\"podTemplate\\\":{\\\"spec\\\":{\\\"containers\\\":[{\\\"image\\\":\\\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\\\",\\\"name\\\":\\\"sql\\\"}]}}}}}\\n\"},\"creationTimestamp\":\"2026-10-30T09:08:59Z\",\"generation\":1,\"name\":\"sql\",\"namespace\":\"managed-bootstrap\",\"resourceVersion\":\"868\",\"uid\":\"a4f321e1-7de2-4852-b37d-2343f2cb3d68\"},\"spec\":{\"automaticRecovery\":\"OFF\",\"deletionPolicy\":\"Orphan\",\"genesisIntent\":\"Create\",\"logicalID\":\"managed-ci\",\"provisionalRetentionSeconds\":172800,\"voterCount\":3,\"workload\":{\"podTemplate\":{\"spec\":{\"containers\":[{\"image\":\"rhiza-managed:e7da4cc91b4bb95f398b51c90094b4da5aa1a0d2-a42831e923c4\",\"name\":\"sql\"}]}}}}}]"

func TestManagedCapturedKubeletObjectsReconcile(t *testing.T) {
	t.Run("kubectl enriched items", func(t *testing.T) { testManagedCapturedKubeletObjectsReconcile(t, false, "") })
	t.Run("native list item type omitted", func(t *testing.T) { testManagedCapturedKubeletObjectsReconcile(t, true, "") })
	for _, fault := range []string{"UID changed", "RV changed", "wrong live type", "partial list type", "live main started"} {
		t.Run(fault, func(t *testing.T) { testManagedCapturedKubeletObjectsReconcile(t, true, fault) })
	}
}

func testManagedCapturedKubeletObjectsReconcile(t *testing.T, nativeList bool, fault string) {
	var objects []map[string]json.RawMessage
	if err := json.Unmarshal([]byte(managedCapturedKubeletObjects), &objects); err != nil {
		t.Fatal(err)
	}
	var cluster, deployment map[string]json.RawMessage
	var pods []map[string]json.RawMessage
	sets := map[string]map[string]json.RawMessage{}
	for _, obj := range objects {
		var kind string
		_ = json.Unmarshal(obj["kind"], &kind)
		var meta struct{ Name string }
		_ = json.Unmarshal(obj["metadata"], &meta)
		switch kind {
		case "RhizaCluster":
			cluster = obj
		case "Deployment":
			deployment = obj
		case "ReplicaSet":
			sets[meta.Name] = obj
		case "Pod":
			pods = append(pods, obj)
		}
	}
	var meta struct{ Name, Namespace, UID string }
	_ = json.Unmarshal(cluster["metadata"], &meta)
	ref := V1Alpha2ClusterRef{Name: meta.Name, Namespace: meta.Namespace, UID: meta.UID}
	var spec struct {
		Workload struct {
			PodTemplate struct {
				Spec struct{ Containers []V1Alpha2Container }
			}
		}
	}
	_ = json.Unmarshal(cluster["spec"], &spec)
	profile := phase9RuntimeProfile()
	profile.InitImage = spec.Workload.PodTemplate.Spec.Containers[0].Image
	puts := 0
	podGets := 0
	store := phase2Store(t, func(r *http.Request) (*http.Response, error) {
		respond := func(v any) (*http.Response, error) { return phase2Response(200, string(mustManagedJSON(v))), nil }
		switch {
		case r.Method == http.MethodPost:
			return phase2Response(409, `{}`), nil
		case strings.Contains(r.URL.Path, "/serviceaccounts/"):
			return respond(managedFixtureServiceAccount(ref))
		case strings.Contains(r.URL.Path, "/secrets/"):
			b, _ := json.Marshal(profile)
			return respond(map[string]any{"metadata": map[string]any{"name": profile.ProfileSecretName, "namespace": ref.Namespace, "uid": "secretuid"}, "data": map[string][]byte{V1Alpha2ManagedRuntimeProfileKey: b}})
		case strings.Contains(r.URL.Path, "/deployments/"):
			return respond(deployment)
		case strings.Contains(r.URL.Path, "/replicasets/"):
			return respond(sets[r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]])
		case strings.HasSuffix(r.URL.Path, "/pods"):
			items := pods
			if nativeList {
				items = make([]map[string]json.RawMessage, 0, len(pods))
				for _, pod := range pods {
					item := map[string]json.RawMessage{}
					for k, v := range pod {
						if k != "kind" && k != "apiVersion" {
							item[k] = v
						}
					}
					items = append(items, item)
					if fault == "partial list type" {
						item["kind"] = json.RawMessage(`"Pod"`)
					}
				}
			}
			return respond(map[string]any{"apiVersion": "v1", "kind": "PodList", "metadata": map[string]any{"resourceVersion": "captured"}, "items": items})
		case strings.Contains(r.URL.Path, "/pods/"):
			podGets++
			for _, pod := range pods {
				var m struct{ Name string }
				_ = json.Unmarshal(pod["metadata"], &m)
				if strings.HasSuffix(r.URL.Path, "/"+m.Name) {
					var live map[string]json.RawMessage
					_ = json.Unmarshal(mustManagedJSON(pod), &live)
					var metadata map[string]any
					_ = json.Unmarshal(live["metadata"], &metadata)
					switch fault {
					case "UID changed":
						metadata["uid"] = "replacement"
					case "RV changed":
						metadata["resourceVersion"] = "changed"
					case "wrong live type":
						live["kind"] = json.RawMessage(`"Service"`)
					case "live main started":
						var status map[string]any
						_ = json.Unmarshal(live["status"], &status)
						status["containerStatuses"].([]any)[0].(map[string]any)["state"] = map[string]any{"running": map[string]any{}}
						live["status"] = mustManagedJSON(status)
					}
					live["metadata"] = mustManagedJSON(metadata)
					return respond(live)
				}
			}
			return phase2Response(404, `{}`), nil
		case r.Method == http.MethodPut:
			puts++
			if err := json.NewDecoder(r.Body).Decode(&cluster); err != nil {
				return nil, err
			}
			var m map[string]any
			_ = json.Unmarshal(cluster["metadata"], &m)
			m["resourceVersion"] = "committed"
			cluster["metadata"] = mustManagedJSON(m)
			return respond(cluster)
		default:
			return respond(cluster)
		}
	})
	store.Kube.Namespace = ref.Namespace
	got, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), store.Kube, ref, profile)
	if fault != "" {
		if !errors.Is(err, ErrV1Alpha2BootstrapPending) || puts != 0 {
			t.Fatalf("accepted native-list drift %s: %v puts=%d", fault, err, puts)
		}
		return
	}
	if err != nil || puts != 1 {
		t.Fatalf("captured kubelet objects failed genesis: %v puts=%d", err, puts)
	}
	if nativeList && podGets != 6 {
		t.Fatalf("native-list items did not receive exact live checks: gets=%d", podGets)
	}
	p, err := got.BootstrapPlan()
	if err != nil || len(p.Members) != 3 {
		t.Fatalf("captured plan: %v", err)
	}
}

// managedFixturePods supplies public status on exact API-owned Pods. It does
// not simulate kubelet execution or prove the init binary ran.
func managedFixturePods(ref V1Alpha2ClusterRef, rsName, rsUID string, spec json.RawMessage) []any {
	var admitted map[string]any
	_ = json.Unmarshal(spec, &admitted)
	managedFixtureAdmitPodToken(admitted)
	spec = mustManagedJSON(admitted)
	pods := make([]any, 0, 3)
	for i := 1; i <= 3; i++ {
		key := base64.StdEncoding.EncodeToString([]byte(strings.Repeat(string(rune('a'+i)), 32)))
		pods = append(pods, map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": map[string]any{"name": fmt.Sprintf("pod-%d", i), "namespace": ref.Namespace, "uid": fmt.Sprintf("poduid-%d", i), "resourceVersion": "4", "ownerReferences": []any{map[string]any{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": rsName, "uid": rsUID, "controller": true}}}, "spec": spec, "status": map[string]any{"podIP": fmt.Sprintf("10.0.0.%d", i), "initContainerStatuses": []any{map[string]any{"name": V1Alpha2PrepareIdentityContainer, "state": map[string]any{"terminated": map[string]any{"exitCode": 0, "message": fmt.Sprintf(`{"version":1,"publicKey":%q}`, key)}}}, map[string]any{"name": V1Alpha2AwaitBootstrapContainer, "state": map[string]any{"running": map[string]any{}}}}}})
	}
	return pods
}

func managedFixtureServiceAccount(ref V1Alpha2ClusterRef) map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": map[string]any{"name": V1Alpha2ManagedHostServiceAccount, "namespace": ref.Namespace, "uid": "host-account-uid", "resourceVersion": "1"}}
}

func managedFixtureAdmitPodToken(spec map[string]any) {
	const name = "kube-api-access-fixture"
	projected := map[string]any{"defaultMode": 420, "sources": []any{map[string]any{"serviceAccountToken": map[string]any{"expirationSeconds": 3607, "path": "token"}}, map[string]any{"configMap": map[string]any{"name": "kube-root-ca.crt", "items": []any{map[string]any{"key": "ca.crt", "path": "ca.crt"}}}}, map[string]any{"downwardAPI": map[string]any{"items": []any{map[string]any{"path": "namespace", "fieldRef": map[string]any{"apiVersion": "v1", "fieldPath": "metadata.namespace"}}}}}}}
	spec["volumes"] = append(spec["volumes"].([]any), map[string]any{"name": name, "projected": projected})
	for _, key := range []string{"containers", "initContainers"} {
		for _, raw := range spec[key].([]any) {
			c := raw.(map[string]any)
			c["volumeMounts"] = append(c["volumeMounts"].([]any), map[string]any{"name": name, "mountPath": "/var/run/secrets/kubernetes.io/serviceaccount", "readOnly": true})
		}
	}
}

func TestManagedInitializationRequiresInstalledHostAccountBeforePost(t *testing.T) {
	for _, name := range []string{"absent", "deleting", "wrong namespace", "wrong name", "missing UID"} {
		t.Run(name, func(t *testing.T) {
			s, kube := phase9Setup(t, phase8Cluster)
			s.serviceAccount = managedFixtureServiceAccount(phase2Ref)
			meta := s.serviceAccount["metadata"].(map[string]any)
			switch name {
			case "absent":
				s.serviceAccountStatus = 404
			case "deleting":
				meta["deletionTimestamp"] = "2026-01-01T00:00:00Z"
			case "wrong namespace":
				meta["namespace"] = "other"
			case "wrong name":
				meta["name"] = "default"
			case "missing UID":
				delete(meta, "uid")
			}
			if _, err := ReconcileV1Alpha2ManagedInitialization(context.Background(), kube, phase2Ref, phase9RuntimeProfile()); err == nil || s.posts != 0 || s.puts != 0 {
				t.Fatalf("invalid host account reached POST: %v posts=%d puts=%d", err, s.posts, s.puts)
			}
		})
	}
}

func TestManagedHostInstallOnlyWritesCRStatus(t *testing.T) {
	b, err := os.ReadFile("../../deploy/operator/rbac.yaml")
	if err != nil {
		t.Fatal(err)
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	account, binding, operatorRead := false, false, false
	grants := map[string]map[string]bool{}
	for {
		var doc map[string]any
		if err := d.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		meta, _ := doc["metadata"].(map[string]any)
		if meta["name"] == V1Alpha2ManagedHostServiceAccount {
			if meta["namespace"] != "rhiza" {
				t.Fatal("host install is not namespace scoped")
			}
			switch doc["kind"] {
			case "ServiceAccount":
				account = true
			case "Role":
				for _, raw := range doc["rules"].([]any) {
					r := raw.(map[string]any)
					verbs := r["verbs"].([]any)
					for _, resource := range r["resources"].([]any) {
						name := resource.(string)
						if grants[name] != nil {
							t.Fatal("duplicate host resource rule")
						}
						grants[name] = map[string]bool{}
						for _, verb := range verbs {
							grants[name][verb.(string)] = true
						}
						if name == "rhizaclusters/status" {
							if len(verbs) != 2 || !grants[name]["get"] || !grants[name]["update"] {
								t.Fatal("host status permissions must be get/update only")
							}
						} else if len(verbs) != 1 || verbs[0] != "get" {
							t.Fatalf("host writes non-status resource %s", name)
						}
					}
				}
			case "RoleBinding":
				ref := doc["roleRef"].(map[string]any)
				subjects := doc["subjects"].([]any)
				if ref["kind"] != "Role" || ref["name"] != V1Alpha2ManagedHostServiceAccount || len(subjects) != 1 {
					t.Fatal("host binding has unexpected role/subjects")
				}
				subject := subjects[0].(map[string]any)
				binding = subject["kind"] == "ServiceAccount" && subject["name"] == V1Alpha2ManagedHostServiceAccount && subject["namespace"] == "rhiza"
			default:
				t.Fatal("unexpected managed host install object")
			}
		}
		if doc["kind"] == "Role" && meta["name"] == "rhiza-operator" {
			for _, raw := range doc["rules"].([]any) {
				r := raw.(map[string]any)
				for _, resource := range r["resources"].([]any) {
					if resource == "serviceaccounts" {
						names := r["resourceNames"].([]any)
						verbs := r["verbs"].([]any)
						operatorRead = len(names) == 1 && names[0] == V1Alpha2ManagedHostServiceAccount && len(verbs) == 1 && verbs[0] == "get"
					}
				}
			}
		}
	}
	if !account || !binding || !operatorRead || len(grants) != 5 {
		t.Fatal("incomplete or overprivileged managed host installation")
	}
	for _, resource := range []string{"rhizaclusters", "rhizaclusters/status", "pods", "replicasets", "deployments"} {
		if !grants[resource]["get"] {
			t.Fatalf("host missing GET %s", resource)
		}
	}
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
