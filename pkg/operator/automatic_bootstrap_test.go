package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http/httptest"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

// automaticBootstrapGenesisFixture starts from the same CONTROL-backed,
// enrolled held-profile fixture used by the frozen-source recovery tests. It
// leaves a real admitted and dispatched empty-HEAD create unresolved, with no
// provider object, so bootstrap tests cannot mistake an absent HEAD for proof
// that no write was attempted.
type automaticBootstrapGenesisFixture struct {
	heldProfileFixture
	archive  *objstore.InMemBucket
	provider *heldGCSServer
	intent   fencing.MutationIntent
	cohort   []fencing.RuntimeBinding
}

const (
	automaticBootstrapClusterName  = "bootstrap-cluster"
	automaticBootstrapClusterUID   = "bootstrap-cluster-uid"
	automaticBootstrapNamespaceUID = "bootstrap-namespace-uid"
)

func newAutomaticBootstrapGenesisFixture(t *testing.T) automaticBootstrapGenesisFixture {
	t.Helper()
	e, api, _, baseRuntime := newMutationStoreFixture(t)
	e.MutationProfile = fencing.MutationHeldExecutionProfile
	e.RequireMutationJournal = true
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	f := heldProfileFixture{e: e, api: api, scope: scope}
	cohort := configureAutomaticBootstrapCohort(t, &f, baseRuntime, 3)
	f.id, f.runtime = fencing.Identity{SourceClusterID: "source", NodeID: "rhiza-0", PodUID: "bootstrap-pod-0", RunID: "bootstrap-run-0", WALIdentity: "bootstrap-wal-0"}, cohort[0]
	if err := e.initializeRoot(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		ledger.RequireRuntimeBinding = true
		ledger.RequireMutationJournal = true
		return ledger.RegisterEnrolled(f.id, f.runtime)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.reserveMutationProducer(t.Context(), f.runtime); err != nil {
		t.Fatal(err)
	}
	f.stages, _ = e.sourceStagesFor(t.Context(), f.runtime)
	if f.stages == nil {
		t.Fatal("bootstrap source stage store was not configured")
	}
	if _, err := e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		if err := ledger.BindMutationScope(f.id, hashJSON(scope), fencing.MutationHeldExecutionProfile); err != nil {
			return err
		}
		if err := ledger.BindMutationStage(f.id, f.stages.incarnation); err != nil {
			return err
		}
		return ledger.Bind(f.id)
	}); err != nil {
		t.Fatal(err)
	}
	producer, err := e.enrolledMutationRun(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	f.producer = producer.MutationProducerUID
	if _, err := e.changeMutationProducer(t.Context(), f.runtime, func(p *fencing.MutationProducer) error { return p.Activate(f.id) }); err != nil {
		t.Fatal(err)
	}
	f.receipts, err = e.receiptsForRuntime(f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	configureAutomaticBootstrapKubernetes(t, &f, cohort)
	archive := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = archive.Close() })
	e.ArchiveBucket = frozenSourceEmptyTestBucket{Bucket: archive}
	if _, err := f.e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
		ledger.ArchiveGenesisState = fencing.ArchiveGenesisRegistrationPending
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.e.claimArchiveScopeForRegistration(t.Context(), f.runtime.StatefulSetUID, f.id, f.runtime); err != nil {
		t.Fatal(err)
	}
	provider := newFrozenEmptyNoEffectServer(t, f)
	payload, err := recovery.EmptyArchiveHeadBytes()
	if err != nil {
		t.Fatal(err)
	}
	intent := f.admitUpload(t, 1, "archive/head.bin", fencing.MutationCondition{Mode: fencing.MutationCreateOnly}, payload)
	if err := f.e.executeSourceMutationRequest(t.Context(), "Bearer test", f.id, sourceMutationRequest{
		ProducerUID: f.producer, Sequence: 1, Intent: intent,
	}, true); !errors.Is(err, ErrFencePending) {
		t.Fatalf("initial held genesis did not remain pending after the provider no-effect fault: %v", err)
	}
	root, err := f.e.read(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	mutationProducer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
	if err != nil {
		t.Fatal(err)
	}
	entry := mutationProducer.Entries[1]
	if root.Ledger.ArchiveGenesisRef == nil || root.Ledger.ArchiveGenesisRef.Mutation == nil ||
		root.Ledger.ArchiveGenesisRef.Mutation.Intent != intent || entry.Intent != intent ||
		entry.Preparation == nil || entry.Preparation.Profile != fencing.MutationHeldExecutionProfile ||
		entry.Resolution != nil || entry.GuardedLogicalResolution != nil || len(entry.Dispatches) != 1 {
		t.Fatalf("fixture lacks exact CONTROL-backed unresolved genesis admission: root=%+v entry=%+v", root.Ledger, entry)
	}
	if exists, err := archive.Exists(t.Context(), "root/source/archive/head.bin"); err != nil || exists {
		t.Fatalf("fixture unexpectedly has an archive HEAD: exists=%t err=%v", exists, err)
	}
	return automaticBootstrapGenesisFixture{heldProfileFixture: f, archive: archive, provider: provider, intent: intent, cohort: cohort}
}

// configureAutomaticBootstrapCohort mirrors the exact enrolled runtime cohort
// into both canonical CONTROL and the live StatefulSet-owned Pod inventory.
// Recovery tests must not rely on missing Pods or membership as a shortcut for
// the bootstrap candidate.
func configureAutomaticBootstrapCohort(t *testing.T, f *heldProfileFixture, base fencing.RuntimeBinding, count int) []fencing.RuntimeBinding {
	t.Helper()
	if count < 1 {
		t.Fatal("bootstrap cohort must contain at least one enrolled runtime")
	}
	cohort := make([]fencing.RuntimeBinding, 0, count)
	for i := range count {
		runtime := base
		runtime.NodeID = fmt.Sprintf("rhiza-%d", i)
		runtime.PodUID = fmt.Sprintf("bootstrap-pod-%d", i)
		runtime.RunID = fmt.Sprintf("bootstrap-run-%d", i)
		runtime.NodeName = fmt.Sprintf("node-%d", i)
		runtime.NodeUID = fmt.Sprintf("node-uid-%d", i)
		runtime.MachineID = fmt.Sprintf("machine-%d", i)
		runtime.BootID = fmt.Sprintf("boot-%d", i)
		runtime.RuntimeID = fmt.Sprintf("runtime-%d", i)
		runtime.SandboxID = fmt.Sprintf("sandbox-%d", i)
		runtime.ContainerID = fmt.Sprintf("container-%d", i)
		runtime.ImageID = fmt.Sprintf("image-%d", i)
		runtime.BindingUID = automaticBootstrapClusterUID
		runtime.NamespaceUID = automaticBootstrapNamespaceUID
		cohort = append(cohort, runtime)
	}
	return cohort
}

func configureAutomaticBootstrapKubernetes(t *testing.T, f *heldProfileFixture, cohort []fencing.RuntimeBinding) {
	t.Helper()
	sts := object{
		"metadata": object{"name": "rhiza", "namespace": "test", "uid": f.runtime.StatefulSetUID},
		"spec": object{
			"replicas": float64(len(cohort)), "ordinals": object{"start": float64(0)}, "volumeClaimTemplates": []any{}, "serviceName": "rhiza-headless",
			"template": object{"spec": object{
				"containers": []any{object{"name": "rhiza", "env": []any{
					object{"name": "RHIZA_CLUSTER_ID", "value": f.runtime.SourceClusterID},
					object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
					object{"name": "RHIZA_OBJSTORE_BUCKET", "value": f.scope.Bucket},
					object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
					object{"name": "RHIZA_OBJSTORE_DURABILITY", "value": "async"},
					object{"name": "RHIZA_ENABLE_RECONFIGURATION", "value": "true"},
					object{"name": "RHIZA_BIND_ADDR", "value": "0.0.0.0:8080"},
					object{"name": "RHIZA_PEER_ADDR", "value": "0.0.0.0:9090"},
					object{"name": "RHIZA_NODE_ID", "valueFrom": object{"fieldRef": object{"fieldPath": "metadata.name"}}},
				}, "volumeMounts": []any{object{"name": "data", "mountPath": "/data"}},
					"command": []any{"/usr/local/bin/rhiza-fencer"}, "args": []any{"supervise", "--", "/usr/local/bin/rhiza"}}},
				"volumes": []any{object{"name": "data", "emptyDir": object{}}},
			}},
		},
	}
	cluster := ClusterResource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaCluster",
		Metadata: object{"name": automaticBootstrapClusterName, "namespace": "test", "uid": automaticBootstrapClusterUID},
		Spec: ClusterSpec{LogicalID: "bootstrap-logical", StatefulSet: "rhiza", Container: "rhiza", AdoptClusterID: f.runtime.SourceClusterID,
			VoterCount: len(cohort), HTTPPort: 8080, PeerPort: 9090, Bootstrap: true, Automatic: true,
			Policy: func() *AutoPolicy { p := goodPolicy; p.AllowDataLoss = true; return &p }()},
	}
	members, err := clusterMemberSources(&cluster, sts, "test")
	if err != nil {
		t.Fatal(err)
	}
	container := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
	container["env"] = append(list(container["env"]), object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(jsonBytes(members))})
	clusterPath := f.e.controller().clusterPath(automaticBootstrapClusterName)
	podList := make([]any, 0, len(cohort))
	for i, runtime := range cohort {
		identity := fencing.Identity{SourceClusterID: runtime.SourceClusterID, NodeID: runtime.NodeID, PodUID: runtime.PodUID,
			RunID: runtime.RunID, WALIdentity: fmt.Sprintf("bootstrap-wal-%d", i)}
		if i > 0 {
			if _, err := f.e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
				return ledger.RegisterEnrolled(identity, runtime)
			}); err != nil {
				t.Fatalf("register bootstrap runtime %d: %v", i, err)
			}
			if _, err := f.e.reserveMutationProducer(t.Context(), runtime); err != nil {
				t.Fatalf("reserve bootstrap mutation producer %d: %v", i, err)
			}
			stages, err := f.e.sourceStagesFor(t.Context(), runtime)
			if err != nil {
				t.Fatalf("create bootstrap stage store %d: %v", i, err)
			}
			if _, err := f.e.change(t.Context(), runtime.StatefulSetUID, runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
				if err := ledger.BindMutationScope(identity, hashJSON(f.scope), fencing.MutationHeldExecutionProfile); err != nil {
					return err
				}
				if err := ledger.BindMutationStage(identity, stages.incarnation); err != nil {
					return err
				}
				return ledger.Bind(identity)
			}); err != nil {
				t.Fatalf("bind bootstrap runtime %d: %v", i, err)
			}
			if _, err := f.e.changeMutationProducer(t.Context(), runtime, func(producer *fencing.MutationProducer) error {
				return producer.Activate(identity)
			}); err != nil {
				t.Fatalf("activate bootstrap producer %d: %v", i, err)
			}
		} else {
			identity = f.id
		}
		pod := independentPod(runtime.NodeID, runtime.PodUID)
		metadata := asObject(pod["metadata"])
		metadata["namespace"] = "test"
		metadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": runtime.StatefulSetUID, "controller": true}}
		asObject(pod["spec"])["nodeName"] = runtime.NodeName
		status := asObject(list(nested(pod, "status", "containerStatuses"))[0])
		status["containerID"], status["imageID"] = "containerd://"+runtime.ContainerID, "sha256:"+runtime.ImageID
		podList = append(podList, pod)
	}
	f.api.mu.Lock()
	sts["spec"].(object)["template"].(object)["metadata"] = object{"labels": object{"app": "rhiza"}}
	f.api.authStatefulSet = sts
	f.api.objects[clusterPath] = jsonObjectForPreflight(t, cluster)
	f.api.objects["/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters"] = object{"items": []any{jsonObjectForPreflight(t, cluster)}}
	f.api.objects["/api/v1/namespaces/test"] = object{"metadata": object{"name": "test", "uid": automaticBootstrapNamespaceUID}}
	f.api.objects[f.e.controller().corePath("services", "rhiza-headless")] = object{"metadata": object{"name": "rhiza-headless", "namespace": "test"}, "spec": object{
		"clusterIP": "None", "publishNotReadyAddresses": true, "selector": object{"app": "rhiza"},
	}}
	f.api.objects[f.e.controller().corePath("pods", "")] = object{"items": podList}
	f.api.mu.Unlock()
	if len(cohort) > 0 && cohort[0].StatefulSetUID != f.runtime.StatefulSetUID {
		t.Fatal("bootstrap cohort StatefulSet UID differs from canonical runtime")
	}
}

func TestBootstrapGenesisPreflightUsesCanonicalControlAndCompleteLiveCohort(t *testing.T) {
	f := newAutomaticBootstrapGenesisFixture(t)
	f.e.ControllerServiceAccount = "default"
	server := httptest.NewTLSServer(f.e.Handler())
	t.Cleanup(server.Close)
	client := &FencingClient{URL: server.URL, TokenFile: f.e.Kube.TokenFile, Client: server.Client()}
	request := BootstrapGenesisPreflightRequest{ClusterName: automaticBootstrapClusterName, ClusterUID: automaticBootstrapClusterUID, StatefulSetUID: f.runtime.StatefulSetUID}

	rootBefore, rootVersion, err := f.e.AuthorityState.ReadRoot(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	producerBefore := make(map[string]fencing.MutationProducer, len(f.cohort))
	for _, runtime := range f.cohort {
		producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), runtime)
		if err != nil {
			t.Fatal(err)
		}
		producerBefore[runtime.RunID] = producer
	}
	response, err := client.BootstrapGenesisPreflight(t.Context(), request)
	if err != nil {
		t.Fatalf("valid held unknown genesis and complete unready cohort were rejected: %v", err)
	}
	if response.Binding.Version != 1 || response.Binding.ClusterName != automaticBootstrapClusterName ||
		response.Binding.ClusterUID != automaticBootstrapClusterUID || !validEvidenceDigest(response.Binding.CandidateDigest) {
		t.Fatalf("preflight returned an incomplete candidate binding: %+v", response.Binding)
	}
	if len(response.Targets) != len(f.cohort) {
		t.Fatalf("preflight captured %d targets, want the full %d-Pod cohort: %+v", len(response.Targets), len(f.cohort), response.Targets)
	}
	for i, target := range response.Targets {
		runtime := f.cohort[i]
		if target.NodeID != runtime.NodeID || target.Pod != runtime.NodeID || target.PodUID != runtime.PodUID ||
			target.WALIdentity != fmt.Sprintf("bootstrap-wal-%d", i) || target.NodeName != runtime.NodeName ||
			target.NodeUID != runtime.NodeUID || target.ContainerID != runtime.ContainerID || target.ImageID != runtime.ImageID {
			t.Fatalf("target %d is not bound to the enrolled CONTROL runtime: target=%+v runtime=%+v", i, target, runtime)
		}
	}
	rootAfter, rootVersionAfter, err := f.e.AuthorityState.ReadRoot(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || hashJSON(rootBefore) != hashJSON(rootAfter) || rootVersion.Value != rootVersionAfter.Value {
		t.Fatalf("read-only candidate preflight changed canonical CONTROL root: err=%v", err)
	}
	for _, runtime := range f.cohort {
		producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), runtime)
		if err != nil || hashJSON(producerBefore[runtime.RunID]) != hashJSON(producer) {
			t.Fatalf("read-only candidate preflight changed CONTROL producer %q: err=%v", runtime.RunID, err)
		}
	}
}

func TestBootstrapGenesisPreflightRequiresExplicitEligiblePolicyAndExactInventory(t *testing.T) {
	for _, name := range []string{"automatic-off", "bootstrap-off", "policy-absent", "loss-not-allowed", "PVC", "missing-pod", "ready-pod", "malformed-owner-reference", "malformed-condition", "malformed-container-status"} {
		t.Run(name, func(t *testing.T) {
			f := newAutomaticBootstrapGenesisFixture(t)
			f.e.ControllerServiceAccount = "default"
			switch name {
			case "automatic-off", "bootstrap-off", "policy-absent", "loss-not-allowed":
				cluster := automaticBootstrapCluster(t, f)
				switch name {
				case "automatic-off":
					cluster.Spec.Automatic = false
				case "bootstrap-off":
					cluster.Spec.Bootstrap = false
				case "policy-absent":
					cluster.Spec.Policy = nil
				case "loss-not-allowed":
					cluster.Spec.Policy.AllowDataLoss = false
				}
				storeAutomaticBootstrapCluster(t, f, cluster)
			case "PVC":
				f.api.mu.Lock()
				asObject(f.api.authStatefulSet["spec"])["volumeClaimTemplates"] = []any{object{"metadata": object{"name": "data"}}}
				f.api.mu.Unlock()
			case "missing-pod", "ready-pod", "malformed-owner-reference", "malformed-condition", "malformed-container-status":
				f.api.mu.Lock()
				pods := list(asObject(f.api.objects[f.e.controller().corePath("pods", "")])["items"])
				if name == "missing-pod" {
					asObject(f.api.objects[f.e.controller().corePath("pods", "")])["items"] = pods[:len(pods)-1]
				} else if name == "ready-pod" {
					conditions := []any{object{"type": "Ready", "status": "True"}}
					asObject(pods[0])["status"] = object{"containerStatuses": []any{object{"containerID": "containerd://container-0", "imageID": "sha256:image-0"}}, "conditions": conditions}
				} else {
					pod := asObject(pods[0])
					switch name {
					case "malformed-owner-reference":
						asObject(pod["metadata"])["ownerReferences"] = []any{"not-an-owner-reference"}
					case "malformed-condition":
						asObject(pod["status"])["conditions"] = []any{"not-a-condition"}
					case "malformed-container-status":
						asObject(pod["status"])["containerStatuses"] = []any{"not-a-container-status"}
					}
				}
				f.api.mu.Unlock()
			}
			if _, err := f.e.bootstrapGenesisPreflight(t.Context(), BootstrapGenesisPreflightRequest{
				ClusterName: automaticBootstrapClusterName, ClusterUID: automaticBootstrapClusterUID, StatefulSetUID: f.runtime.StatefulSetUID,
			}); err == nil {
				t.Fatal("bootstrap preflight accepted an ineligible policy or incomplete live inventory")
			}
		})
	}
}

func TestBootstrapGenesisPreflightRequiresTheCanonicalControlRoot(t *testing.T) {
	f := newAutomaticBootstrapGenesisFixture(t)
	request := BootstrapGenesisPreflightRequest{ClusterName: automaticBootstrapClusterName, ClusterUID: automaticBootstrapClusterUID, StatefulSetUID: f.runtime.StatefulSetUID}
	// The valid fixture has neither a provider HEAD nor membership. It remains
	// eligible only because CONTROL binds the exact held genesis tuple.
	if _, err := f.e.bootstrapGenesisPreflight(t.Context(), request); err != nil {
		t.Fatalf("complete CONTROL-bound held genesis was rejected: %v", err)
	}
	rootKey, _, err := f.e.AuthorityState.rootKey(f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.e.AuthorityState.bucket.Delete(t.Context(), rootKey); err != nil {
		t.Fatal(err)
	}
	if _, err := f.e.bootstrapGenesisPreflight(t.Context(), request); err == nil {
		t.Fatal("missing CONTROL root was treated as bootstrap evidence because the archive HEAD and membership were absent")
	}
}

func TestBootstrapGenesisFenceRejectsCandidateChangesBeforeClosingRoot(t *testing.T) {
	for _, name := range []string{"policy", "inventory", "root"} {
		t.Run(name, func(t *testing.T) {
			f := newAutomaticBootstrapGenesisFixture(t)
			response, err := f.e.bootstrapGenesisPreflight(t.Context(), BootstrapGenesisPreflightRequest{
				ClusterName: automaticBootstrapClusterName, ClusterUID: automaticBootstrapClusterUID, StatefulSetUID: f.runtime.StatefulSetUID,
			})
			if err != nil {
				t.Fatalf("bootstrap candidate preflight failed: %v", err)
			}
			cluster := automaticBootstrapCluster(t, f)
			switch name {
			case "policy":
				cluster.Spec.Policy.CooldownSeconds++
				storeAutomaticBootstrapCluster(t, f, cluster)
			case "inventory":
				f.api.mu.Lock()
				pods := list(asObject(f.api.objects[f.e.controller().corePath("pods", "")])["items"])
				asObject(pods[0])["metadata"].(object)["uid"] = "replacement-pod-uid"
				f.api.mu.Unlock()
			case "root":
				if _, err := f.e.change(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID, func(ledger *fencing.Ledger) error {
					ledger.RequireMutationJournal = false
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			request := FenceRequest{LogicalID: cluster.Spec.LogicalID, BindingUID: automaticBootstrapClusterUID, Namespace: "test",
				StatefulSet: cluster.Spec.StatefulSet, StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: f.runtime.SourceClusterID,
				OperationID: "bootstrap-genesis-close", Scope: ScopeGeneration, Targets: response.Targets, BootstrapGenesis: &response.Binding}
			record := &fenceResource{Metadata: object{"name": "bootstrap-fence", "namespace": "test"}, Spec: request}
			if err := f.e.reconcileFence(t.Context(), v2FenceEndpoint(), record); err == nil {
				t.Fatalf("fence closed a changed automatic bootstrap %s candidate", name)
			}
			root, err := f.e.read(t.Context(), f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			producer, _, _, err := f.e.AuthorityState.ReadProducer(t.Context(), f.runtime)
			if err != nil {
				t.Fatal(err)
			}
			if root.Ledger.ClosedBy != "" || producer.Phase != fencing.MutationOpen || record.Status.Proof != nil {
				t.Fatalf("changed candidate produced a closed root or proof: root=%+v producer=%+v proof=%+v", root.Ledger, producer, record.Status.Proof)
			}
			f.api.mu.Lock()
			defer f.api.mu.Unlock()
			if f.api.statusWrites != 0 {
				t.Fatalf("changed candidate caused %d fence status writes", f.api.statusWrites)
			}
		})
	}
}

func automaticBootstrapCluster(t *testing.T, f automaticBootstrapGenesisFixture) ClusterResource {
	t.Helper()
	var cluster ClusterResource
	if err := json.Unmarshal(jsonBytes(f.api.objects[f.e.controller().clusterPath(automaticBootstrapClusterName)]), &cluster); err != nil {
		t.Fatal(err)
	}
	return cluster
}

func storeAutomaticBootstrapCluster(t *testing.T, f automaticBootstrapGenesisFixture, cluster ClusterResource) {
	t.Helper()
	encoded := jsonObjectForPreflight(t, cluster)
	f.api.mu.Lock()
	defer f.api.mu.Unlock()
	f.api.objects[f.e.controller().clusterPath(automaticBootstrapClusterName)] = encoded
	f.api.objects["/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters"] = object{"items": []any{encoded}}
}

func automaticBootstrapController(t *testing.T, f automaticBootstrapGenesisFixture) (*Controller, *httptest.Server, ClusterResource) {
	t.Helper()
	f.e.ControllerServiceAccount = "default"
	f.e.ArchiveStoreIdentity = StoreIdentity(map[string]string{
		"RHIZA_OBJSTORE_PROVIDER": "gcs", "RHIZA_OBJSTORE_BUCKET": f.scope.Bucket, "RHIZA_OBJSTORE_PREFIX": "root",
	})
	server := httptest.NewTLSServer(f.e.Handler())
	t.Cleanup(server.Close)
	client := &FencingClient{URL: server.URL, TokenFile: f.e.Kube.TokenFile, Client: server.Client()}
	cluster := automaticBootstrapCluster(t, f)
	normalizeClusterSpec(&cluster)
	storeAutomaticBootstrapCluster(t, f, cluster)
	var sts object
	if err := f.e.Kube.Get(t.Context(), f.e.controller().stsPath("rhiza"), &sts); err != nil {
		t.Fatal(err)
	}
	candidate := expectedBootstrapCandidate(&cluster, sts)
	if err := f.archive.Upload(t.Context(), path.Join("root", "automatic-bootstrap", "test", "rhiza.json"), bytes.NewReader(jsonBytes(candidate)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	container := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
	env, err := (&Controller{Kube: f.e.Kube}).environment(t.Context(), container)
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{Fencer: client, Automatic: true, Kube: f.e.Kube, Bucket: f.archive, Prefix: "root", StoreIdentity: StoreIdentity(env)}
	return controller, server, cluster
}

func TestAutomaticControllerRecognizesMissingMembershipOnlyWithCanonicalBootstrapEvidence(t *testing.T) {
	t.Run("valid control preflight creates protected fence intent", func(t *testing.T) {
		f := newAutomaticBootstrapGenesisFixture(t)
		controller, _, cluster := automaticBootstrapController(t, f)
		if exists, err := f.archive.Exists(t.Context(), "root/source/voters/membership.json"); err != nil || exists {
			t.Fatalf("fixture must start without source membership: exists=%t err=%v", exists, err)
		}
		if err := controller.reconcileCluster(t.Context(), &cluster); err != nil {
			t.Fatal(err)
		}
		state, _, err := controller.loadAuto(t.Context(), controller.autoKey(&cluster))
		if err != nil || state == nil || state.BootstrapGenesis == nil || state.Intent != nil {
			t.Fatalf("authenticated missing-membership bootstrap did not persist its protected pre-fence state: state=%+v err=%v", state, err)
		}
		state.SuspectedSince = time.Now().Add(-24 * time.Hour)
		_, version, err := controller.loadAuto(t.Context(), controller.autoKey(&cluster))
		if err != nil || version == nil {
			t.Fatalf("reload protected bootstrap state before CAS update: version=%v err=%v", version, err)
		}
		if err := controller.saveAuto(t.Context(), &cluster, state, version); err != nil {
			t.Fatalf("persist elapsed bootstrap failure grace: %v", err)
		}
		if err := controller.reconcileCluster(t.Context(), &cluster); err != nil {
			t.Fatal(err)
		}
		state, _, err = controller.loadAuto(t.Context(), controller.autoKey(&cluster))
		if err != nil || state == nil || state.Intent == nil || state.Intent.Request.BootstrapGenesis == nil ||
			*state.Intent.Request.BootstrapGenesis != *state.BootstrapGenesis || state.Intent.Request.Scope != ScopeGeneration {
			t.Fatalf("controller did not durably reserve the exact canonical bootstrap generation fence: state=%+v err=%v", state, err)
		}
		f.api.mu.Lock()
		child := f.api.objects[controller.resourcePath(state.Intent.RecoveryName)]
		f.api.mu.Unlock()
		if child != nil {
			t.Fatal("controller created a recovery child before persisting a canonical CONTROL fence proof")
		}
	})

	for _, name := range []string{"missing-control-root", "malformed-membership", "membership-read-error"} {
		t.Run(name, func(t *testing.T) {
			f := newAutomaticBootstrapGenesisFixture(t)
			controller, _, cluster := automaticBootstrapController(t, f)
			switch name {
			case "missing-control-root":
				key, _, err := f.e.AuthorityState.rootKey(f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.e.AuthorityState.bucket.Delete(t.Context(), key); err != nil {
					t.Fatal(err)
				}
			case "malformed-membership":
				if err := f.archive.Upload(t.Context(), "root/source/voters/membership.json", strings.NewReader("{"), objstore.WithIfNotExists()); err != nil {
					t.Fatal(err)
				}
			case "membership-read-error":
				controller.Bucket = membershipReadErrorBucket{Bucket: f.archive, key: "root/source/voters/membership.json"}
			}
			if err := controller.reconcileCluster(t.Context(), &cluster); err != nil {
				t.Fatal(err)
			}
			state, _, err := controller.loadAuto(t.Context(), controller.autoKey(&cluster))
			if state != nil {
				t.Fatalf("%s was treated as missing-membership proof or created protected bootstrap state: state=%+v err=%v", name, state, err)
			}
		})
	}
}

// TestAutomaticRecoveryChildReconcilesMissingMembershipFromCanonicalBootstrap
// exercises the actual child reconciler (not only its preflight helpers). The
// source has no membership object; the only permitted cohort comes from the
// exact, authenticated CONTROL fence proof and the bootstrap cluster's
// Secret-backed declared members.
func TestAutomaticRecoveryChildReconcilesMissingMembershipFromCanonicalBootstrap(t *testing.T) {
	f := newAutomaticBootstrapGenesisFixture(t)
	controller, client, child, request, proof := closeAutomaticBootstrapForChild(t, f)
	ctx := t.Context()

	if exists, err := f.archive.Exists(ctx, "root/source/voters/membership.json"); err != nil || exists {
		t.Fatalf("test setup unexpectedly created source membership: exists=%t err=%v", exists, err)
	}
	rootBefore, rootVersion, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}

	// Reconcile persists the missing-membership exception and then crosses the
	// old membership gate only after checking both the durable child binding and
	// the authenticated historical CONTROL proof.
	for step := 0; step < 4; step++ {
		if err := controller.reconcile(ctx, &child); err != nil {
			t.Fatalf("controller reconcile step %d rejected the canonical bootstrap child: %v", step, err)
		}
		persistTestRecovery(t, f, child)
		if child.Status.Stage == "Stopping" {
			break
		}
	}
	if child.Status.RecoveryMode != RecoveryModeFrozenSource || child.Status.Source != request.SourceClusterID ||
		child.Status.SourceDurability != "async" || child.Status.TargetBaseKind != "empty" ||
		child.Status.FrozenEmptyGenesisPending || child.Status.Stage != "Stopping" || child.Status.Target == "" ||
		child.Status.SecretName == "" || child.Status.RecoveredTip != 0 {
		t.Fatalf("controller did not progress the confirmed missing-membership recovery through canonical empty-target authorization: %+v", child.Status)
	}
	if child.Status.SourceMembership != sourceFingerprint(request.SourceClusterID, "async", bootstrapMembersFromCluster(t, f), "root/source") {
		t.Fatal("controller did not derive the target membership fingerprint from the declared bootstrap cohort")
	}
	if exists, err := f.archive.Exists(ctx, "root/source/voters/membership.json"); err != nil || exists {
		t.Fatalf("controller wrote or exposed synthetic source membership: exists=%t err=%v", exists, err)
	}
	if exists, err := f.archive.Exists(ctx, path.Join("root", child.Status.Target, "voters/membership.json")); err != nil || !exists {
		t.Fatalf("Secret-backed target membership was not created: exists=%t err=%v", exists, err)
	}
	var targetRoot fenceLedger
	if targetRoot, err = f.e.read(ctx, f.runtime.StatefulSetUID, child.Status.Target); err != nil ||
		targetRoot.Ledger.EmptyRecoveryAuthorization == nil || targetRoot.Ledger.ArchiveGenesisRef != nil {
		t.Fatalf("controller crossed the membership gate without persisting only an authorized, uncommitted empty target: root=%+v err=%v", targetRoot.Ledger, err)
	}
	rootAfter, rootVersionAfter, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || hashJSON(rootBefore) != hashJSON(rootAfter) || rootVersion == nil || rootVersionAfter == nil || *rootVersion != *rootVersionAfter {
		t.Fatalf("controller reconciliation changed canonical source CONTROL state: err=%v", err)
	}

	// The same historical CONTROL proof must remain confirmable after the live
	// StatefulSet and same-named Pods belong to the successor. This endpoint is
	// a read-only proof lookup, not a fresh source-cohort preflight.
	activateBootstrapTargetFixture(t, f, child.Status.Target)
	confirmation, err := client.ConfirmBootstrapGenesisFence(ctx, request)
	if err != nil || hashJSON(confirmation.Request) != hashJSON(request) || hashJSON(confirmation.Proof) != hashJSON(*proof) {
		t.Fatalf("persisted CONTROL proof became unavailable after target activation/source Pod replacement: confirmation=%+v err=%v", confirmation, err)
	}
	rootAfterConfirm, rootVersionAfterConfirm, err := f.e.AuthorityState.ReadRoot(ctx, f.runtime.StatefulSetUID, f.runtime.SourceClusterID)
	if err != nil || hashJSON(rootBefore) != hashJSON(rootAfterConfirm) || rootVersionAfterConfirm == nil || *rootVersion != *rootVersionAfterConfirm {
		t.Fatalf("historical proof confirmation mutated source CONTROL state: err=%v", err)
	}
}

func TestAutomaticRecoveryChildRejectsMissingControlCohortChangesAndMalformedMembership(t *testing.T) {
	for _, name := range []string{"missing-control-root", "mismatched-live-cohort", "malformed-membership"} {
		t.Run(name, func(t *testing.T) {
			f := newAutomaticBootstrapGenesisFixture(t)
			controller, _, child, request, _ := closeAutomaticBootstrapForChild(t, f)
			switch name {
			case "missing-control-root":
				key, _, err := f.e.AuthorityState.rootKey(request.StatefulSetUID, request.SourceClusterID)
				if err != nil {
					t.Fatal(err)
				}
				if err := f.e.AuthorityState.bucket.Delete(t.Context(), key); err != nil {
					t.Fatal(err)
				}
			case "mismatched-live-cohort":
				members := bootstrapMembersFromCluster(t, f)
				container := asObject(list(nested(f.api.authStatefulSet, "spec", "template", "spec", "containers"))[0])
				container["env"] = replaceEnv(list(container["env"]), "RHIZA_CLUSTER_MEMBERS", object{"value": string(jsonBytes(members[:len(members)-1]))})
			case "malformed-membership":
				if err := f.archive.Upload(t.Context(), "root/source/voters/membership.json", strings.NewReader("{"), objstore.WithIfNotExists()); err != nil {
					t.Fatal(err)
				}
			}
			err := controller.reconcile(t.Context(), &child)
			if name == "missing-control-root" {
				if err == nil {
					t.Fatal("controller accepted a DATA-side proof after the canonical CONTROL root disappeared")
				}
			} else if err != nil || child.Status.Phase != "Blocked" {
				t.Fatalf("controller did not fail closed for %s: phase=%q message=%q err=%v", name, child.Status.Phase, child.Status.Message, err)
			}
			if exists, err := f.archive.Exists(t.Context(), "root/source/voters/membership.json"); err != nil {
				t.Fatal(err)
			} else if name != "malformed-membership" && exists {
				t.Fatal("controller created source membership while rejecting bootstrap evidence")
			}
			if child.Status.Stage != "" || child.Status.Target != "" {
				t.Fatalf("rejected source evidence advanced target recovery: %+v", child.Status)
			}
			scope := fencing.ArchiveScope{Provider: "gcs", Bucket: f.scope.Bucket, Prefix: "root/source", ClusterID: request.SourceClusterID}
			scopeHash, err := scope.Hash()
			if err != nil {
				t.Fatal(err)
			}
			targetID, err := recovery.FrozenTargetID(recovery.FrozenSourceIdentity{ScopeHash: scopeHash, ClusterID: request.SourceClusterID, StatefulSetUID: request.StatefulSetUID})
			if err != nil {
				t.Fatal(err)
			}
			targetScope := fencing.ArchiveScope{Provider: "gcs", Bucket: f.scope.Bucket, Prefix: path.Join("root", targetID), ClusterID: targetID}
			if _, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(t.Context(), targetScope); err == nil {
				t.Fatal("rejected source evidence created canonical target claim")
			}
			rootKey, _, err := f.e.AuthorityState.rootKey(request.StatefulSetUID, targetID)
			if err != nil {
				t.Fatal(err)
			}
			if exists, err := f.e.AuthorityState.bucket.Exists(t.Context(), rootKey); err != nil || exists {
				t.Fatalf("rejected source evidence created target root: exists=%t err=%v", exists, err)
			}
		})
	}
}

func closeAutomaticBootstrapForChild(t *testing.T, f automaticBootstrapGenesisFixture) (*Controller, *FencingClient, Resource, FenceRequest, *FenceProof) {
	t.Helper()
	ctx := t.Context()
	controller, _, cluster := automaticBootstrapController(t, f)
	client := controller.Fencer.(*FencingClient)
	preflight, err := client.BootstrapGenesisPreflight(ctx, BootstrapGenesisPreflightRequest{
		ClusterName: automaticBootstrapClusterName, ClusterUID: automaticBootstrapClusterUID, StatefulSetUID: f.runtime.StatefulSetUID,
	})
	if err != nil {
		t.Fatalf("canonical bootstrap preflight: %v", err)
	}
	request := FenceRequest{LogicalID: cluster.Spec.LogicalID, BindingUID: automaticBootstrapClusterUID, Namespace: "test",
		StatefulSet: cluster.Spec.StatefulSet, StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: f.runtime.SourceClusterID,
		OperationID: "bootstrap-recovery-child", Scope: ScopeGeneration, Targets: append([]FenceTarget(nil), preflight.Targets...), BootstrapGenesis: &preflight.Binding}
	f.e.CollectRuntimeEvidence = func(_ context.Context, runtime fencing.RuntimeBinding, stop fencing.StopRequest) ([]byte, error) {
		for i, expected := range f.cohort {
			if runtime == expected && stop.Identity.RunID == expected.RunID {
				return []byte("bootstrap-terminal:" + fmt.Sprint(i)), nil
			}
		}
		return nil, fmt.Errorf("unexpected bootstrap terminal collection target")
	}
	f.e.VerifyRuntimeEvidence = func(_ context.Context, runtime fencing.RuntimeBinding, stop fencing.StopRequest, receipt []byte) error {
		for i, expected := range f.cohort {
			if runtime == expected && stop.Identity.RunID == expected.RunID && string(receipt) == "bootstrap-terminal:"+fmt.Sprint(i) {
				return nil
			}
		}
		return fmt.Errorf("bootstrap terminal receipt differs from the enrolled runtime")
	}
	fenceName := "fence-" + hashJSON([]string{request.BindingUID, request.OperationID})[:40]
	fence := &fenceResource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaFence",
		Metadata: object{"name": fenceName, "namespace": "test", "uid": "bootstrap-fence-uid", "resourceVersion": "1"}, Spec: request}
	if err := f.e.reconcileFence(ctx, v2FenceEndpoint(), fence); err != nil {
		t.Fatalf("reconcile real CONTROL generation fence: %v", err)
	}
	if fence.Status.Proof == nil || fence.Status.Proof.IndependentV2 == nil {
		t.Fatal("fencer did not persist an IndependentV2 proof")
	}
	fenceData, err := json.Marshal(fence)
	if err != nil {
		t.Fatal(err)
	}
	var fenceObject object
	if err := json.Unmarshal(fenceData, &fenceObject); err != nil {
		t.Fatal(err)
	}
	f.api.mu.Lock()
	f.api.objects[v2FenceEndpoint()+"/"+fenceName] = fenceObject
	f.api.objects[v2FenceEndpoint()] = object{"items": []any{fenceObject}}
	f.api.mu.Unlock()

	// The two fixture clients represent the same test bucket: expose the exact
	// provider-created bytes to the archive reader without inventing a separate
	// HEAD or membership object.
	f.provider.mu.Lock()
	providerHead, exists := f.provider.objects[path.Join(f.scope.Prefix, "archive/head.bin")]
	f.provider.mu.Unlock()
	if !exists {
		t.Fatal("closure did not settle the exact held source HEAD in the provider fixture")
	}
	if err := f.archive.Upload(ctx, path.Join(f.scope.Prefix, "archive/head.bin"), bytes.NewReader(providerHead.payload), objstore.WithIfNotExists()); err != nil {
		t.Fatalf("make exact provider HEAD visible to the archive reader: %v", err)
	}

	rootAfterFence, _, err := f.e.AuthorityState.ReadRoot(ctx, request.StatefulSetUID, request.SourceClusterID)
	if err != nil || rootAfterFence.Ledger.ClosedBy != request.OperationID || rootAfterFence.Ledger.ClosedHash != fence.Status.Proof.RequestHash {
		t.Fatalf("source root is not closed by the exact canonical request: root=%+v err=%v", rootAfterFence.Ledger, err)
	}

	clusterPath := controller.clusterPath(automaticBootstrapClusterName)
	var storedCluster ClusterResource
	if err := json.Unmarshal(jsonBytes(f.api.objects[clusterPath]), &storedCluster); err != nil {
		t.Fatal(err)
	}
	sts := f.api.authStatefulSet
	container := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
	env, err := controller.environment(ctx, container)
	if err != nil {
		t.Fatal(err)
	}
	members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		t.Fatal(err)
	}
	voters, err := derivedVoters(&storedCluster, sts)
	if err != nil {
		t.Fatal(err)
	}
	identities := make(map[quepaxa.NodeID]FenceTarget, len(preflight.Targets))
	for _, target := range preflight.Targets {
		identities[quepaxa.NodeID(target.NodeID)] = target
	}
	spec := Spec{StatefulSet: "rhiza", Container: "rhiza", SourceClusterID: f.runtime.SourceClusterID, Durability: "async",
		RecoveryID: request.OperationID, AllowDataLoss: true, Fence: Fence{RecoveryID: request.OperationID, ClusterID: f.runtime.SourceClusterID,
			StatefulSetUID: f.runtime.StatefulSetUID, Confirmed: true, Evidence: fence.Status.Proof.ProofID}}
	child := Resource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaRecovery",
		Metadata: object{"name": request.OperationID, "namespace": "test", "uid": "bootstrap-child-uid", "generation": float64(1),
			"annotations": object{automaticOwner: automaticBootstrapClusterUID, automaticLogical: storedCluster.Spec.LogicalID}}, Spec: spec,
		Status: Status{RecoveryMode: RecoveryModeFrozenSource, RecoveryID: request.OperationID, SpecHash: immutableSpecHash(spec),
			StatefulSetUID: f.runtime.StatefulSetUID, Source: f.runtime.SourceClusterID, SourceDurability: "async",
			SourceMembership: sourceFingerprint(f.runtime.SourceClusterID, "async", members, "root/source"),
			TargetBaseKind:   "empty", FrozenEmptyGenesisPending: false}}
	autoState := &autoControl{Version: 1, BindingUID: automaticBootstrapClusterUID, BindingHash: autoBinding(&storedCluster),
		BindingVersion: autoBindingVersion, StatefulSetUID: f.runtime.StatefulSetUID, ActiveClusterID: f.runtime.SourceClusterID,
		Voters: voters, Identities: identities, BootstrapGenesis: &preflight.Binding,
		Intent: &autoIntent{Request: request, Policy: *storedCluster.Spec.Policy, Durability: "async", Voters: voters,
			Proof: fence.Status.Proof, RecoveryName: request.OperationID, RecoveryUID: str(child.Metadata["uid"]), SpecHash: hashJSON(child.Spec)}}
	if err := controller.saveAuto(ctx, &storedCluster, autoState, nil); err != nil {
		t.Fatalf("persist matching automatic bootstrap intent: %v", err)
	}
	persistTestRecovery(t, f, child)
	return controller, client, child, request, fence.Status.Proof
}

func persistTestRecovery(t *testing.T, f automaticBootstrapGenesisFixture, resource Resource) {
	t.Helper()
	data, err := json.Marshal(resource)
	if err != nil {
		t.Fatal(err)
	}
	var value object
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	f.api.mu.Lock()
	f.api.objects[f.e.controller().resourcePath(resourceName(&resource))] = value
	f.api.mu.Unlock()
}

func bootstrapMembersFromCluster(t *testing.T, f automaticBootstrapGenesisFixture) []quepaxa.Member {
	t.Helper()
	container := list(nested(f.api.authStatefulSet, "spec", "template", "spec", "containers"))[0]
	env, err := (&Controller{Kube: f.e.Kube}).environment(t.Context(), asObject(container))
	if err != nil {
		t.Fatal(err)
	}
	members, err := recoveryMembers(env["RHIZA_CLUSTER_MEMBERS"])
	if err != nil {
		t.Fatal(err)
	}
	return members
}

func activateBootstrapTargetFixture(t *testing.T, f automaticBootstrapGenesisFixture, target string) {
	t.Helper()
	f.api.mu.Lock()
	defer f.api.mu.Unlock()
	sts := f.api.authStatefulSet
	container := asObject(list(nested(sts, "spec", "template", "spec", "containers"))[0])
	container["env"] = replaceEnv(list(container["env"]), "RHIZA_CLUSTER_ID", object{"value": target})
	f.api.objects[f.e.controller().stsPath("rhiza")] = sts
	pods := list(asObject(f.api.objects[f.e.controller().corePath("pods", "")])["items"])
	for i, pod := range pods {
		metadata := asObject(asObject(pod)["metadata"])
		metadata["uid"] = fmt.Sprintf("successor-pod-%d", i)
	}
}

type membershipReadErrorBucket struct {
	objstore.Bucket
	key string
}

func (b membershipReadErrorBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if name == b.key {
		return nil, errors.New("injected membership read failure")
	}
	return b.Bucket.Get(ctx, name)
}

type bootstrapControlFenceConfirmer struct {
	// records models the independently persisted CONTROL fence records. It is
	// intentionally not backed by the controller's automatic DATA bucket.
	records map[string]BootstrapGenesisFenceConfirmation
	calls   int
}

func (f *bootstrapControlFenceConfirmer) Fence(context.Context, FenceRequest) (*FenceProof, error) {
	return nil, errors.New("unexpected fence dispatch from child authorization")
}

func (f *bootstrapControlFenceConfirmer) ConfirmBootstrapGenesisFence(_ context.Context, request FenceRequest) (BootstrapGenesisFenceConfirmation, error) {
	f.calls++
	record, ok := f.records[hashJSON(request)]
	if !ok || hashJSON(record.Request) != hashJSON(request) {
		return BootstrapGenesisFenceConfirmation{}, errors.New("canonical CONTROL fence record is missing or differs")
	}
	return record, nil
}

func TestBootstrapChildRequiresExactCanonicalControlProof(t *testing.T) {
	for _, tc := range []struct {
		name                string
		controlPresent      bool
		alterControlProof   bool
		alterControlBinding bool
		wantAuthorized      bool
	}{
		{name: "missing canonical proof"},
		{name: "different valid proof", controlPresent: true, alterControlProof: true},
		{name: "different candidate binding", controlPresent: true, alterControlBinding: true},
		{name: "exact canonical proof", controlPresent: true, wantAuthorized: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket := autoBucket(t)
			t.Cleanup(func() { _ = bucket.Close() })
			c := autoController(bucket)
			cluster := autoClusterResource("bootstrap-cluster", "bootstrap-cluster-uid", "bootstrap-logical")
			binding := BootstrapGenesisBinding{Version: 1, CandidateDigest: strings.Repeat("a", 64), ClusterName: "bootstrap-cluster", ClusterUID: "bootstrap-cluster-uid"}
			request := FenceRequest{LogicalID: cluster.Spec.LogicalID, BindingUID: "automatic-binding", Namespace: "default",
				StatefulSet: "rhiza", StatefulSetUID: "bootstrap-sts-uid", SourceClusterID: "source", OperationID: "bootstrap-recovery-1",
				Scope: ScopeGeneration, BootstrapGenesis: &binding, Targets: []FenceTarget{
					{NodeID: "rhiza-0", Pod: "rhiza-0", PodUID: "pod-0", WALIdentity: "wal-0"},
					{NodeID: "rhiza-1", Pod: "rhiza-1", PodUID: "pod-1", WALIdentity: "wal-1"},
					{NodeID: "rhiza-2", Pod: "rhiza-2", PodUID: "pod-2", WALIdentity: "wal-2"},
				}}
			dataProof := bootstrapIndependentProof(t, request)
			controlRequest := request
			controlProof := *dataProof
			controlEvidence := *dataProof.IndependentV2
			controlEvidence.Terminals = append([]fencing.TerminalReference(nil), dataProof.IndependentV2.Terminals...)
			controlProof.IndependentV2 = &controlEvidence
			if tc.alterControlProof {
				controlProof.IndependentV2.Terminals[0].RuntimeHash = strings.Repeat("9", 64)
				controlProof.ProofID = "independent-v2-" + hashJSON(controlProof.IndependentV2)
			}
			if tc.alterControlBinding {
				changed := *controlRequest.BootstrapGenesis
				changed.CandidateDigest = strings.Repeat("b", 64)
				controlRequest.BootstrapGenesis = &changed
				controlProof = *bootstrapIndependentProof(t, controlRequest)
			}
			confirmer := &bootstrapControlFenceConfirmer{records: make(map[string]BootstrapGenesisFenceConfirmation)}
			if tc.controlPresent {
				confirmer.records[hashJSON(controlRequest)] = BootstrapGenesisFenceConfirmation{Request: controlRequest, Proof: controlProof}
			}
			c.Fencer = confirmer

			desired := Spec{StatefulSet: request.StatefulSet, Container: "rhiza", SourceClusterID: request.SourceClusterID,
				Durability: "async", RecoveryID: request.OperationID, AllowDataLoss: true}
			state := validAutoControl()
			state.BindingUID = request.BindingUID
			state.StatefulSetUID = request.StatefulSetUID
			state.ActiveClusterID = request.SourceClusterID
			state.BootstrapGenesis = &binding
			state.Intent = &autoIntent{RecoveryName: request.OperationID, RecoveryUID: "recovery-resource-uid", SpecHash: hashJSON(desired),
				Request: request, Proof: dataProof}
			if err := c.saveAuto(t.Context(), cluster, state, nil); err != nil {
				t.Fatal(err)
			}
			child := &Resource{Metadata: map[string]any{"name": request.OperationID, "namespace": "default", "uid": "recovery-resource-uid",
				"annotations": map[string]any{automaticOwner: state.BindingUID, automaticLogical: cluster.Spec.LogicalID}}, Spec: desired}
			authorized, err := c.authorizeAutomaticChild(t.Context(), child)
			if tc.wantAuthorized {
				if err != nil || !authorized {
					t.Fatalf("exact canonical CONTROL proof did not authorize child: authorized=%t err=%v", authorized, err)
				}
				if confirmer.calls != 1 {
					t.Fatalf("canonical CONTROL was checked %d times, want once", confirmer.calls)
				}
				return
			}
			if err == nil || authorized {
				t.Fatalf("proof-shaped DATA record authorized child without exact CONTROL confirmation: authorized=%t err=%v", authorized, err)
			}
			if confirmer.calls != 1 {
				t.Fatalf("canonical CONTROL was checked %d times, want one fail-closed check", confirmer.calls)
			}
		})
	}
}

func TestBootstrapChildConfirmerReadsCanonicalControlOverAuthenticatedHTTP(t *testing.T) {
	for _, name := range []string{"missing-control-fence", "different-control-proof"} {
		t.Run(name, func(t *testing.T) {
			f := newAutomaticBootstrapGenesisFixture(t)
			f.e.ControllerServiceAccount = "default"
			f.e.ArchiveStoreIdentity = StoreIdentity(map[string]string{
				"RHIZA_OBJSTORE_PROVIDER": "gcs", "RHIZA_OBJSTORE_BUCKET": f.scope.Bucket, "RHIZA_OBJSTORE_PREFIX": "root",
			})
			server := httptest.NewTLSServer(f.e.Handler())
			t.Cleanup(server.Close)
			client := &FencingClient{URL: server.URL, TokenFile: f.e.Kube.TokenFile, Client: server.Client()}
			preflight, err := client.BootstrapGenesisPreflight(t.Context(), BootstrapGenesisPreflightRequest{
				ClusterName: automaticBootstrapClusterName, ClusterUID: automaticBootstrapClusterUID, StatefulSetUID: f.runtime.StatefulSetUID,
			})
			if err != nil {
				t.Fatalf("prepare canonical request binding: %v", err)
			}
			request := FenceRequest{LogicalID: "bootstrap-logical", BindingUID: automaticBootstrapClusterUID, Namespace: "test",
				StatefulSet: "rhiza", StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: f.runtime.SourceClusterID,
				OperationID: "bootstrap-rpc-operation", Scope: ScopeGeneration, Targets: preflight.Targets, BootstrapGenesis: &preflight.Binding}
			requestHash, err := requestHash(&request)
			if err != nil {
				t.Fatal(err)
			}
			nodes := make([]string, 0, len(request.Targets))
			for _, target := range request.Targets {
				nodes = append(nodes, target.NodeID)
			}
			if _, err := f.e.change(t.Context(), request.StatefulSetUID, request.SourceClusterID, func(ledger *fencing.Ledger) error {
				return ledger.CloseNodes(request.OperationID, requestHash, nodes)
			}); err != nil {
				t.Fatalf("persist exact CONTROL closure for RPC fixture: %v", err)
			}
			if name == "different-control-proof" {
				wrong := bootstrapIndependentProof(t, request)
				wrong.IndependentV2.LedgerHash = strings.Repeat("f", 64)
				wrong.ProofID = "independent-v2-" + hashJSON(wrong.IndependentV2)
				fenceName := "fence-" + hashJSON([]string{request.BindingUID, request.OperationID})[:40]
				fence := &fenceResource{Metadata: object{"name": fenceName, "namespace": "test"}, Spec: request}
				fence.Status.Proof = wrong
				f.api.mu.Lock()
				f.api.objects[v2FenceEndpoint()+"/"+fenceName] = jsonObjectForPreflight(t, fence)
				f.api.mu.Unlock()
			}

			controller := &Controller{Fencer: client, Kube: f.e.Kube, Bucket: f.archive, Prefix: "root"}
			dataProof := bootstrapIndependentProof(t, request)
			state := validAutoControl()
			state.BindingUID, state.StatefulSetUID, state.ActiveClusterID = request.BindingUID, request.StatefulSetUID, request.SourceClusterID
			state.BootstrapGenesis = request.BootstrapGenesis
			desired := Spec{StatefulSet: request.StatefulSet, Container: "rhiza", SourceClusterID: request.SourceClusterID,
				Durability: "async", RecoveryID: request.OperationID, AllowDataLoss: true}
			state.Intent = &autoIntent{RecoveryName: request.OperationID, RecoveryUID: "bootstrap-child-uid", SpecHash: hashJSON(desired), Request: request, Proof: dataProof}
			cluster := autoClusterResource(automaticBootstrapClusterName, automaticBootstrapClusterUID, request.LogicalID)
			if err := controller.saveAuto(t.Context(), cluster, state, nil); err != nil {
				t.Fatal(err)
			}
			child := &Resource{Metadata: map[string]any{"name": request.OperationID, "namespace": "default", "uid": "bootstrap-child-uid",
				"annotations": map[string]any{automaticOwner: state.BindingUID, automaticLogical: request.LogicalID}}, Spec: desired}

			before, beforeVersion, err := f.e.AuthorityState.ReadRoot(t.Context(), request.StatefulSetUID, request.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			authorized, authErr := controller.authorizeAutomaticChild(t.Context(), child)
			if authErr == nil && authorized {
				t.Fatal("DATA-resident shaped proof authorized a child without the exact canonical CONTROL fence proof")
			}
			after, afterVersion, err := f.e.AuthorityState.ReadRoot(t.Context(), request.StatefulSetUID, request.SourceClusterID)
			if err != nil {
				t.Fatal(err)
			}
			if hashJSON(before) != hashJSON(after) || beforeVersion == nil || afterVersion == nil || *beforeVersion != *afterVersion {
				t.Fatalf("authenticated read-only confirmer changed CONTROL source root: before=%+v after=%+v", beforeVersion, afterVersion)
			}
		})
	}
}

func bootstrapIndependentProof(t *testing.T, request FenceRequest) *FenceProof {
	t.Helper()
	hash, err := requestHash(&request)
	if err != nil {
		t.Fatal(err)
	}
	scope := AttestationStoreScope{Provider: "gcs", Bucket: "archive", Prefix: "root/source"}
	evidence := &IndependentEvidenceV2{Version: 2, RequestHash: hash, LedgerHash: strings.Repeat("1", 64), StorageScope: scope}
	for i, target := range request.Targets {
		identity := fencing.Identity{SourceClusterID: request.SourceClusterID, NodeID: target.NodeID, PodUID: target.PodUID,
			RunID: fmt.Sprintf("bootstrap-run-%d", i), WALIdentity: target.WALIdentity}
		stop := fencing.StopRequest{OperationID: "stop-" + identity.RunID, RequestHash: strings.Repeat("2", 64), Identity: identity}
		evidence.Terminals = append(evidence.Terminals, fencing.TerminalReference{EvidenceVersion: 2, Request: stop,
			RuntimeHash: strings.Repeat("3", 64), ProducerUID: "producer-" + identity.RunID, ProofName: "proof-" + identity.RunID,
			ProofUID: "proof-uid-" + identity.RunID, ProofDigest: strings.Repeat("4", 64), RuntimeReceiptDigest: strings.Repeat("5", 64),
			MutationReceiptDigest: strings.Repeat("6", 64), StorageScopeDigest: hashJSON(scope)})
	}
	return &FenceProof{OperationID: request.OperationID, SourceClusterID: request.SourceClusterID, BindingUID: request.BindingUID,
		RequestHash: hash, ProofID: "independent-v2-" + hashJSON(evidence), ProcessesTerminated: true, RecreationBlocked: true,
		StorageQuiesced: true, IndependentV2: evidence}
}
