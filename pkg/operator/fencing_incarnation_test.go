package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func registeredLearnerLedgerFixture(t *testing.T, e *FencingExecutor, workloadUID, source string, identity fencing.Identity) (string, object) {
	t.Helper()
	ledger := fencing.Ledger{SourceClusterID: source}
	if err := ledger.Register(identity); err != nil {
		t.Fatal(err)
	}
	endpoint := e.ledgerPath(workloadUID, source)
	resource := object{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": object{"name": path.Base(endpoint), "namespace": e.Kube.Namespace, "uid": "ledger-uid", "resourceVersion": "1"},
		"data":     object{"ledger.json": string(jsonBytes(fenceLedger{StatefulSetUID: workloadUID, Ledger: ledger}))},
	}
	return endpoint, resource
}

func TestIncarnationCredentialIsUIDBoundAndReused(t *testing.T) {
	ctx := context.Background()
	bucket := autoBucket(t)
	c := autoController(bucket)
	root := autoClusterResource("root", "root-uid", "logical")
	root.Spec.Automatic = true
	root.Spec.Container = "rhiza"
	root.Status.ActiveClusterID = "source"
	root.Status.ObservedGeneration = 1
	state := validAutoControl()
	state.BindingUID, state.BindingHash = "root-uid", autoBinding(root)
	state.BindingVersion = autoBindingVersion
	state.StatefulSetUID, state.ActiveClusterID = "sts-uid", "source"
	if err := c.saveAuto(ctx, root, state, nil); err != nil {
		t.Fatal(err)
	}
	binding := object{"logicalID": root.Spec.LogicalID, "bindingUID": state.BindingUID, "bindingHash": state.BindingHash}
	if err := bucket.Upload(ctx, path.Join(c.Prefix, "automatic-workloads", c.Kube.Namespace, state.StatefulSetUID, "binding.json"), strings.NewReader(string(jsonBytes(binding)))); err != nil {
		t.Fatal(err)
	}
	members := []quepaxa.Member{voterMember("source", "rhiza-0", "token-0"), voterMember("source", "rhiza-1", "token-1"), voterMember("source", "rhiza-2", "token-2")}
	record := recovery.NewMembershipRecord("source", members, "before-ack")
	if err := bucket.Upload(ctx, path.Join(c.Prefix, "source", "voters", "membership.json"), strings.NewReader(string(jsonBytes(record)))); err != nil {
		t.Fatal(err)
	}
	sts := object{
		"metadata": object{"name": "rhiza", "uid": "sts-uid", "generation": float64(1)},
		"spec": object{
			"serviceName": "rhiza-headless",
			"template": object{
				"metadata": object{"annotations": object{fencingProtocolAnnotation: "1"}},
				"spec": object{"containers": []any{object{
					"name": "rhiza",
					"env": []any{
						object{"name": "RHIZA_CLUSTER_ID", "value": "source"},
						object{"name": "RHIZA_ADMIN_TOKEN", "value": "admin"},
						object{"name": "RHIZA_BIND_ADDR", "value": "0.0.0.0:8080"},
						object{"name": "RHIZA_PEER_ADDR", "value": "0.0.0.0:9090"},
						object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(jsonBytes(members))},
						object{"name": "RHIZA_OBJSTORE_DURABILITY", "value": "before-ack"},
					},
				}}},
			},
		},
	}
	pod := supervisedTestPod()
	metadata := asObject(pod["metadata"])
	metadata["name"], metadata["uid"] = "rhiza-2", "pod-uid-new"
	metadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": true}}
	asObject(pod["spec"])["serviceAccountName"] = "rhiza"
	container := asObject(list(nested(pod, "spec", "containers"))[0])
	container["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
	secrets := map[string]object{}
	creates := 0
	ledgerPath := (&FencingExecutor{Kube: c.Kube}).ledgerPath("sts-uid", "source")
	var ledgerObject object
	ledgerWrites := 0
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == ledgerPath:
			if ledgerObject == nil {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(ledgerObject)
		case r.Method == http.MethodPut && r.URL.Path == ledgerPath:
			var next object
			if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
				t.Error(err)
				http.Error(w, "invalid ConfigMap", http.StatusBadRequest)
				return
			}
			ledgerWrites++
			asObject(next["metadata"])["resourceVersion"] = "2"
			ledgerObject = next
			_ = json.NewEncoder(w).Encode(ledgerObject)
		case r.Method == http.MethodGet && r.URL.Path == c.corePath("pods", "rhiza-2"):
			_ = json.NewEncoder(w).Encode(pod)
		case r.Method == http.MethodGet && r.URL.Path == c.stsPath("rhiza"):
			_ = json.NewEncoder(w).Encode(sts)
		case r.Method == http.MethodGet && r.URL.Path == c.clusterPath(""):
			_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, c.corePath("secrets", "")):
			name := strings.TrimPrefix(r.URL.Path, c.corePath("secrets", "")+"/")
			if secret := secrets[name]; secret != nil {
				_ = json.NewEncoder(w).Encode(secret)
				return
			}
			http.NotFound(w, r)
		case r.Method == http.MethodPost && r.URL.Path == c.corePath("secrets", ""):
			var secret object
			if err := json.NewDecoder(r.Body).Decode(&secret); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			creates++
			asObject(secret["metadata"])["namespace"] = c.Kube.Namespace
			asObject(secret["metadata"])["uid"] = "credential-uid"
			secrets[str(nested(secret, "metadata", "name"))] = secret
			_ = json.NewEncoder(w).Encode(secret)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	c.Kube = &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenPath, Client: api.Client()}
	e := &FencingExecutor{Kube: c.Kube, Bucket: c.Bucket, Prefix: c.Prefix}
	canonical, err := fencing.ReplacementNodeID("pod-uid-new")
	if err != nil {
		t.Fatal(err)
	}
	candidate := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-2", PodName: "rhiza-2", PodUID: "pod-uid-new", RunID: "run-1"}
	identity := candidate
	identity.NodeID = canonical
	first, err := e.incarnationCredential(ctx, candidate, "sts-uid", identity, true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Identity.NodeID != canonical || first.Identity.CredentialUID != "credential-uid" || first.PeerToken == "" || len(first.Learner) == 0 {
		t.Fatalf("incomplete replacement registration: %+v", first)
	}
	registeredLedger := fencing.Ledger{SourceClusterID: "source"}
	if err := registeredLedger.Register(first.Identity); err != nil {
		t.Fatal(err)
	}
	ledgerObject = object{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": object{"name": path.Base(ledgerPath), "namespace": c.Kube.Namespace, "uid": "ledger-uid", "resourceVersion": "1"},
		"data":     object{"ledger.json": string(jsonBytes(fenceLedger{StatefulSetUID: "sts-uid", Ledger: registeredLedger}))},
	}
	canonicalCandidate := candidate
	canonicalCandidate.NodeID = canonical
	registered, err := e.registerIncarnation(ctx, canonicalCandidate, "sts-uid")
	if err != nil || registered.Identity != first.Identity || registered.PeerToken != first.PeerToken || string(registered.Learner) != string(first.Learner) || ledgerWrites != 1 {
		t.Fatalf("exact existing enrollment was not idempotently admitted: registration=%+v writes=%d err=%v", registered, ledgerWrites, err)
	}
	duplicateRun := canonicalCandidate
	duplicateRun.RunID = "run-2"
	if _, err := e.registerIncarnation(ctx, duplicateRun, "sts-uid"); err == nil || !strings.Contains(err.Error(), "previous run for this Pod UID has no terminal receipt") || ledgerWrites != 1 {
		t.Fatalf("second live run for the same Pod UID passed production enrollment: writes=%d err=%v", ledgerWrites, err)
	}
	newUID := candidate
	newUID.PodUID, newUID.RunID = "pod-uid-next", "run-next"
	newUID.NodeID, _ = fencing.ReplacementNodeID(newUID.PodUID)
	if _, err := e.registerIncarnation(ctx, newUID, "sts-uid"); err == nil || !strings.Contains(err.Error(), "no terminal receipt") || ledgerWrites != 1 {
		t.Fatalf("new Pod UID reused the old slot identity: writes=%d err=%v", ledgerWrites, err)
	}
	second, err := e.incarnationCredential(ctx, candidate, "sts-uid", identity, false)
	if err != nil {
		t.Fatal(err)
	}
	if creates != 1 || second.Identity.CredentialUID != first.Identity.CredentialUID || second.PeerToken != first.PeerToken || string(second.Learner) != string(first.Learner) {
		t.Fatalf("credential was not idempotently reused: creates=%d first=%+v second=%+v", creates, first, second)
	}
	metadata["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: "root-uid", ownerAnnotation: "op-1", fencingCredentialAnnotation: "template-secret-uid"}
	metadata["labels"] = object{"rhiza.mrchypark.dev/automatic-learner": "rhiza-2"}
	asObject(list(nested(pod, "metadata", "ownerReferences"))[0])["controller"] = false
	container["name"] = "rhiza"
	credentialName, credentialUID, ledgerUID, err := e.learnerCredentialBindingWithLedgerUID(ctx, pod, "rhiza")
	if err != nil || credentialName != first.Identity.CredentialName || credentialUID != first.Identity.CredentialUID || ledgerUID != "ledger-uid" {
		t.Fatalf("generated registered credential/ledger did not resolve for its exact Pod incarnation: name=%q uid=%q ledgerUID=%q err=%v", credentialName, credentialUID, ledgerUID, err)
	}
	delete(secrets, first.Identity.CredentialName)
	if _, err := e.incarnationCredential(ctx, candidate, "sts-uid", identity, false); err == nil || creates != 1 {
		t.Fatalf("missing registered credential was silently recreated: creates=%d err=%v", creates, err)
	}
}

func TestRegistrationCandidateUsesTokenReviewPodName(t *testing.T) {
	pod := supervisedTestPod()
	metadata := asObject(pod["metadata"])
	metadata["name"], metadata["uid"] = "rhiza-2", "pod-uid-2"
	metadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": true}}
	asObject(pod["spec"])["serviceAccountName"] = "rhiza"
	container := asObject(list(nested(pod, "spec", "containers"))[0])
	container["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
	sts := object{"metadata": object{"uid": "sts-uid"}}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/apis/authentication.k8s.io/v1/tokenreviews":
			_ = json.NewEncoder(w).Encode(object{"status": object{
				"authenticated": true, "audiences": []any{"rhiza-fencer"},
				"user": object{
					"username": "system:serviceaccount:default:rhiza",
					"extra": object{
						"authentication.kubernetes.io/pod-uid":  []any{"pod-uid-2"},
						"authentication.kubernetes.io/pod-name": []any{"rhiza-2"},
					},
				},
			}})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/namespaces/default/pods/rhiza-2":
			_ = json.NewEncoder(w).Encode(pod)
		case r.Method == http.MethodGet && r.URL.Path == "/apis/apps/v1/namespaces/default/statefulsets/rhiza":
			_ = json.NewEncoder(w).Encode(sts)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("service-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	kube := &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenPath, Client: api.Client()}
	e := &FencingExecutor{Kube: kube}
	candidate := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-2", PodUID: "pod-uid-2", RunID: "run-2"}
	var authenticatedName string
	uid, err := e.authenticate(context.Background(), "Bearer caller-token", candidate, true, &authenticatedName)
	if err != nil || uid != "sts-uid" || authenticatedName != "rhiza-2" {
		t.Fatalf("authenticated registration binding = (%q, %q), %v", uid, authenticatedName, err)
	}
}

func TestRegistrationRefusesMissingLedgerWithoutBootstrap(t *testing.T) {
	controller := autoController(nil)
	writes, unrelatedGets := 0, 0
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, controller.corePath("configmaps", "rhiza-fence-ledger-")) {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodPost || r.Method == http.MethodPut {
			writes++
			http.Error(w, "unexpected mutation", http.StatusInternalServerError)
			return
		}
		unrelatedGets++
		http.NotFound(w, r)
	}))
	defer api.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	controller.Kube = &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenPath, Client: api.Client()}
	e := &FencingExecutor{Kube: controller.Kube}
	candidate := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodName: "rhiza-0", PodUID: "pod-uid", RunID: "run-1"}
	if _, err := e.registerIncarnation(context.Background(), candidate, "sts-uid"); err == nil || !strings.Contains(err.Error(), "cold bootstrap is unsupported") {
		t.Fatalf("missing ledger without enrollment was not refused: %v", err)
	}
	if writes != 0 || unrelatedGets != 0 {
		t.Fatalf("missing ledger triggered writes=%d unrelated reads=%d", writes, unrelatedGets)
	}
}

func TestRootlessBucketlessColdOldIDRegistrationIsRejected(t *testing.T) {
	controller := autoController(nil)
	ledgerPath := (&FencingExecutor{Kube: controller.Kube}).ledgerPath("sts-uid", "source")
	pod := supervisedTestPod()
	metadata := asObject(pod["metadata"])
	metadata["name"], metadata["uid"] = "rhiza-0", "pod-uid"
	metadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": true}}
	asObject(pod["spec"])["serviceAccountName"] = "rhiza"
	container := asObject(list(nested(pod, "spec", "containers"))[0])
	container["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
	ledgerGets, rootReads, writes := 0, 0, 0
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/apis/authentication.k8s.io/v1/tokenreviews":
			_ = json.NewEncoder(w).Encode(object{"status": object{
				"authenticated": true, "audiences": []any{"rhiza-fencer"},
				"user": object{"username": "system:serviceaccount:default:rhiza", "extra": object{
					"authentication.kubernetes.io/pod-uid":  []any{"pod-uid"},
					"authentication.kubernetes.io/pod-name": []any{"rhiza-0"},
				}},
			}})
		case r.Method == http.MethodGet && r.URL.Path == controller.corePath("pods", "rhiza-0"):
			_ = json.NewEncoder(w).Encode(pod)
		case r.Method == http.MethodGet && r.URL.Path == controller.stsPath("rhiza"):
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"uid": "sts-uid"}})
		case r.Method == http.MethodGet && r.URL.Path == ledgerPath:
			ledgerGets++
			http.NotFound(w, r)
		case strings.Contains(r.URL.Path, "rhizaclusters"):
			rootReads++
			http.NotFound(w, r)
		case r.Method == http.MethodPost || r.Method == http.MethodPut:
			writes++
			http.Error(w, "unexpected write", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	controller.Kube = &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenPath, Client: api.Client()}
	e := &FencingExecutor{Kube: controller.Kube, Bucket: nil}
	authority := httptest.NewTLSServer(e.Handler())
	defer authority.Close()
	client := &fencing.AuthorityClient{URL: authority.URL + "/v1/supervisor", TokenFile: tokenPath, Client: authority.Client()}
	candidate := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodName: "rhiza-0", PodUID: "pod-uid", RunID: "run-1"}
	if _, err := client.Register(context.Background(), candidate); err == nil {
		t.Fatal("rootless, bucketless cold legacy registration was accepted")
	}
	if ledgerGets != 1 || rootReads != 0 || writes != 0 {
		t.Fatalf("cold admission reads ledger=%d roots=%d writes=%d", ledgerGets, rootReads, writes)
	}
}

func TestRegistrationRequiresRetainedSlotEnrollment(t *testing.T) {
	for _, scenario := range []string{"empty warm ledger", "other node only", "closure only"} {
		t.Run(scenario, func(t *testing.T) {
			controller := autoController(nil)
			ledgerPath := (&FencingExecutor{Kube: controller.Kube}).ledgerPath("sts-uid", "source")
			ledger := fencing.Ledger{SourceClusterID: "source"}
			switch scenario {
			case "other node only":
				if err := ledger.Register(fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-1", PodUID: "pod-1", RunID: "run-1"}); err != nil {
					t.Fatal(err)
				}
			case "closure only":
				if err := ledger.CloseNodes("close-other", "hash", []string{"rhiza-1"}); err != nil {
					t.Fatal(err)
				}
			}
			state := fenceLedger{StatefulSetUID: "sts-uid", Ledger: ledger}
			writes, unexpectedGets := 0, 0
			api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.Path == ledgerPath {
					_ = json.NewEncoder(w).Encode(object{
						"metadata": object{"name": path.Base(ledgerPath), "namespace": "default", "uid": "ledger-uid", "resourceVersion": "1"},
						"data":     object{"ledger.json": string(jsonBytes(state))},
					})
					return
				}
				if r.Method == http.MethodPost || r.Method == http.MethodPut {
					writes++
					http.Error(w, "unexpected registration write", http.StatusInternalServerError)
					return
				}
				unexpectedGets++
				http.NotFound(w, r)
			}))
			defer api.Close()
			tokenPath := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
				t.Fatal(err)
			}
			e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenPath, Client: api.Client()}}
			candidate := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodName: "rhiza-0", PodUID: "new-pod-uid", RunID: "new-run"}
			if _, err := e.registerIncarnation(context.Background(), candidate, "sts-uid"); err == nil || !strings.Contains(err.Error(), "no retained enrollment") {
				t.Fatalf("%s admitted without an exact slot history: %v", scenario, err)
			}
			if writes != 0 || unexpectedGets != 0 {
				t.Fatalf("%s performed writes=%d unexpected reads=%d", scenario, writes, unexpectedGets)
			}
		})
	}
}

func TestLegacyRegistrationRefusesWarmLedgerWithoutManagedEnrollment(t *testing.T) {
	controller := autoController(nil)
	identity := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "pod-uid", RunID: "run-1"}
	ledger := fencing.Ledger{SourceClusterID: "source"}
	if err := ledger.Register(identity); err != nil {
		t.Fatal(err)
	}
	state := fenceLedger{StatefulSetUID: "sts-uid", Ledger: ledger}
	pod := object{"metadata": object{
		"name": "rhiza-0", "uid": "pod-uid",
		"ownerReferences": []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": true}},
	}}
	ledgerPath := (&FencingExecutor{Kube: controller.Kube}).ledgerPath("sts-uid", "source")
	writes, rootReads := 0, 0
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == ledgerPath:
			_ = json.NewEncoder(w).Encode(object{
				"metadata": object{"name": path.Base(ledgerPath), "namespace": "default", "uid": "ledger-uid", "resourceVersion": "1"},
				"data":     object{"ledger.json": string(jsonBytes(state))},
			})
		case r.Method == http.MethodGet && r.URL.Path == controller.corePath("pods", "rhiza-0"):
			_ = json.NewEncoder(w).Encode(pod)
		case r.Method == http.MethodGet && r.URL.Path == controller.clusterPath(""):
			rootReads++
			_ = json.NewEncoder(w).Encode(object{"items": []any{}})
		case r.Method == http.MethodPost || r.Method == http.MethodPut:
			writes++
			http.Error(w, "unexpected registration write", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	controller.Kube = &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenPath, Client: api.Client()}
	e := &FencingExecutor{Kube: controller.Kube, Bucket: nil}
	candidate := identity
	candidate.PodName = "rhiza-0"
	if _, err := e.registerIncarnation(context.Background(), candidate, "sts-uid"); err == nil || !strings.Contains(err.Error(), "lacks verified managed enrollment") {
		t.Fatalf("warm legacy ledger without root/store evidence was admitted: %v", err)
	}
	if rootReads != 1 || writes != 0 {
		t.Fatalf("unverified legacy admission rootReads=%d writes=%d", rootReads, writes)
	}
}

func TestRegistrationCASRefusesDeletedReplacedOrUnenrolledLedger(t *testing.T) {
	for _, scenario := range []string{"deleted after conflict", "replaced after conflict", "history removed after conflict"} {
		t.Run(scenario, func(t *testing.T) {
			controller := autoController(nil)
			ledgerPath := (&FencingExecutor{Kube: controller.Kube}).ledgerPath("sts-uid", "source")
			enrolled := fencing.Identity{SourceClusterID: "source", NodeID: "prior-node", PodUID: "prior-pod", RunID: "prior-run"}
			initial := fencing.Ledger{SourceClusterID: "source"}
			if err := initial.Register(enrolled); err != nil {
				t.Fatal(err)
			}
			current := object{
				"metadata": object{"name": path.Base(ledgerPath), "namespace": "default", "uid": "ledger-original", "resourceVersion": "1"},
				"data":     object{"ledger.json": string(jsonBytes(fenceLedger{StatefulSetUID: "sts-uid", Ledger: initial}))},
			}
			gets, writes, posts := 0, 0, 0
			api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == ledgerPath:
					gets++
					if current == nil {
						http.NotFound(w, r)
						return
					}
					_ = json.NewEncoder(w).Encode(current)
				case r.Method == http.MethodPut:
					writes++
					if writes == 1 {
						if scenario == "deleted after conflict" {
							current = nil
						} else if scenario == "replaced after conflict" {
							current = object{
								"metadata": object{"name": path.Base(ledgerPath), "namespace": "default", "uid": "ledger-replacement", "resourceVersion": "1"},
								"data":     object{"ledger.json": string(jsonBytes(fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source"}}))},
							}
						} else {
							current["metadata"] = object{"name": path.Base(ledgerPath), "namespace": "default", "uid": "ledger-original", "resourceVersion": "2"}
							current["data"] = object{"ledger.json": string(jsonBytes(fenceLedger{StatefulSetUID: "sts-uid", Ledger: fencing.Ledger{SourceClusterID: "source"}}))}
						}
						http.Error(w, "resourceVersion conflict", http.StatusConflict)
						return
					}
					http.Error(w, "unexpected second write", http.StatusInternalServerError)
				case r.Method == http.MethodPost:
					posts++
					http.Error(w, "creation forbidden", http.StatusInternalServerError)
				default:
					http.NotFound(w, r)
				}
			}))
			defer api.Close()
			tokenPath := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
				t.Fatal(err)
			}
			e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenPath, Client: api.Client()}}
			identity := fencing.Identity{SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: "run"}
			validate := func(l *fencing.Ledger) error {
				run, ok := l.Runs[enrolled.RunID]
				if !ok || run.Identity != enrolled {
					return errors.New("retained slot enrollment changed")
				}
				return nil
			}
			_, err := e.changeExisting(context.Background(), "sts-uid", "source", "ledger-original", validate, func(l *fencing.Ledger) error {
				return l.Register(identity)
			})
			if err == nil {
				t.Fatal("ledger deletion or replacement during CAS was accepted")
			}
			if gets != 2 || writes != 1 || posts != 0 {
				t.Fatalf("CAS sequence gets=%d writes=%d posts=%d err=%v", gets, writes, posts, err)
			}
		})
	}
}

func TestBucketlessExactRunStopDrainAndExitRemainAvailable(t *testing.T) {
	controller := autoController(nil)
	identity := fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "pod-uid", RunID: "run-1"}
	bound := identity
	bound.WALIdentity = "wal-identity"
	ledger := fencing.Ledger{SourceClusterID: "source"}
	if err := ledger.Register(identity); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Bind(bound); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Close("close-op", "close-hash"); err != nil {
		t.Fatal(err)
	}
	stop := *ledger.Runs[identity.RunID].Stop
	currentLedger := object{
		"metadata": object{"name": path.Base((&FencingExecutor{Kube: controller.Kube}).ledgerPath("sts-uid", "source")), "namespace": "default", "uid": "ledger-uid", "resourceVersion": "1"},
		"data":     object{"ledger.json": string(jsonBytes(fenceLedger{StatefulSetUID: "sts-uid", Ledger: ledger}))},
	}
	ledgerPath := (&FencingExecutor{Kube: controller.Kube}).ledgerPath("sts-uid", "source")
	pod := supervisedTestPod()
	metadata := asObject(pod["metadata"])
	metadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": true}}
	asObject(pod["spec"])["serviceAccountName"] = "rhiza"
	container := asObject(list(nested(pod, "spec", "containers"))[0])
	container["env"] = []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}}
	api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/apis/authentication.k8s.io/v1/tokenreviews":
			_ = json.NewEncoder(w).Encode(object{"status": object{
				"authenticated": true, "audiences": []any{"rhiza-fencer"},
				"user": object{"username": "system:serviceaccount:default:rhiza", "extra": object{
					"authentication.kubernetes.io/pod-uid":  []any{"pod-uid"},
					"authentication.kubernetes.io/pod-name": []any{"rhiza-0"},
				}},
			}})
		case r.Method == http.MethodGet && r.URL.Path == ledgerPath:
			_ = json.NewEncoder(w).Encode(currentLedger)
		case r.Method == http.MethodPut && r.URL.Path == ledgerPath:
			var next object
			if err := json.NewDecoder(r.Body).Decode(&next); err != nil {
				t.Error(err)
				http.Error(w, "invalid ledger", http.StatusBadRequest)
				return
			}
			asObject(next["metadata"])["resourceVersion"] = "2"
			currentLedger = next
			_ = json.NewEncoder(w).Encode(currentLedger)
		case r.Method == http.MethodGet && r.URL.Path == controller.corePath("pods", "rhiza-0"):
			_ = json.NewEncoder(w).Encode(pod)
		case r.Method == http.MethodGet && r.URL.Path == controller.stsPath("rhiza"):
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"uid": "sts-uid"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("token"), 0o600); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: &Kubernetes{BaseURL: api.URL, Namespace: "default", TokenFile: tokenPath, Client: api.Client()}, Bucket: nil}
	call := func(action string, drain *fencing.DrainReceipt, request *fencing.StopRequest) *httptest.ResponseRecorder {
		t.Helper()
		body := jsonBytes(fencing.AuthorityMessage{Action: action, Identity: bound, Drain: drain, Request: request})
		req := httptest.NewRequest(http.MethodPost, "/v1/supervisor", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer caller-token")
		recorder := httptest.NewRecorder()
		e.Handler().ServeHTTP(recorder, req)
		return recorder
	}
	stopResponse := call("stop", nil, nil)
	if stopResponse.Code != http.StatusOK {
		t.Fatalf("bucketless exact stop status=%d body=%s", stopResponse.Code, stopResponse.Body.String())
	}
	var returnedStop fencing.StopRequest
	if err := json.Unmarshal(stopResponse.Body.Bytes(), &returnedStop); err != nil || returnedStop != stop {
		t.Fatalf("bucketless stop receipt=%+v err=%v", returnedStop, err)
	}
	drain := fencing.DrainReceipt{Request: stop, WritersJoined: true}
	if response := call("drain", &drain, nil); response.Code != http.StatusOK {
		t.Fatalf("bucketless exact drain status=%d body=%s", response.Code, response.Body.String())
	}
	if response := call("exit", nil, &stop); response.Code != http.StatusOK {
		t.Fatalf("bucketless exact exit status=%d body=%s", response.Code, response.Body.String())
	}
	var final fenceLedger
	if err := json.Unmarshal([]byte(str(nested(currentLedger, "data", "ledger.json"))), &final); err != nil {
		t.Fatal(err)
	}
	run := final.Ledger.Runs[identity.RunID]
	if run.Drain == nil || !run.Drain.Clean() || !run.Exited || run.Identity != bound {
		t.Fatalf("bucketless settlement did not retain exact receipt: %+v", run)
	}
}

func TestManagedLegacyIdentityRequiresCurrentObservedPodAndWAL(t *testing.T) {
	ctx := context.Background()
	bucket := autoBucket(t)
	c := autoController(bucket)
	root := autoClusterResource("cluster", "root-uid", "logical")
	root.Spec.Automatic = true
	root.Status.ActiveClusterID = "source"
	state := validAutoControl()
	state.BindingUID, state.BindingHash, state.BindingVersion = "root-uid", autoBinding(root), autoBindingVersion
	state.StatefulSetUID, state.ActiveClusterID = "sts-uid", "source"
	target := FenceTarget{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-uid", WALIdentity: strings.Repeat("a", 64)}
	state.Identities[quepaxa.NodeID(target.NodeID)] = target
	if err := c.saveAuto(ctx, root, state, nil); err != nil {
		t.Fatal(err)
	}
	members := []quepaxa.Member{voterMember("source", "rhiza-0", "token-0"), voterMember("source", "rhiza-1", "token-1"), voterMember("source", "rhiza-2", "token-2")}
	record := recovery.NewMembershipRecord("source", members, "before-ack")
	if err := bucket.Upload(ctx, path.Join(c.Prefix, "source", "voters", "membership.json"), strings.NewReader(string(jsonBytes(record)))); err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{Kube: c.Kube, Bucket: bucket, Prefix: c.Prefix}
	binding := peerClosureBinding{root: *root, environment: map[string]string{
		"RHIZA_CLUSTER_MEMBERS":     string(jsonBytes(members)),
		"RHIZA_OBJSTORE_DURABILITY": "before-ack",
	}}
	identity := fencing.Identity{SourceClusterID: "source", NodeID: target.NodeID, PodUID: target.PodUID, RunID: "run-1"}
	if err := e.validateManagedLegacyIdentity(ctx, binding, identity, target.WALIdentity); err != nil {
		t.Fatalf("exact observed legacy identity rejected: %v", err)
	}
	wrongUID := identity
	wrongUID.PodUID = "replacement-uid"
	if err := e.validateManagedLegacyIdentity(ctx, binding, wrongUID, target.WALIdentity); err == nil {
		t.Fatal("legacy registration accepted a different Pod UID")
	}
	if err := e.validateManagedLegacyIdentity(ctx, binding, identity, strings.Repeat("b", 64)); err == nil {
		t.Fatal("legacy bind accepted a different WAL identity")
	}
}
