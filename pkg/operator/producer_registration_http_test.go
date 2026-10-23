package operator

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/fencing/cri"
	"github.com/mrchypark/rhiza/pkg/fencing/runtimeagent"
	"github.com/thanos-io/objstore"
)

type supervisorRegistrationAgentAuth struct{}

func (supervisorRegistrationAgentAuth) Authenticate(_ context.Context, token string) (runtimeagent.Principal, error) {
	if token != "test" {
		return runtimeagent.Principal{}, fmt.Errorf("unexpected runtime-agent token")
	}
	return runtimeagent.Principal{
		Username: "system:serviceaccount:test:runtime-agent", PodUID: "runtime-agent-pod",
		WorkloadUID: "runtime-agent-workload", Audiences: []string{"rhiza-runtime-agent"},
	}, nil
}

type supervisorRegistrationAgentBackend struct{}

func (supervisorRegistrationAgentBackend) Enroll(_ context.Context, base fencing.RuntimeBinding) (fencing.RuntimeBinding, json.RawMessage, error) {
	base.NodeUID = "node-uid"
	base.MachineID = "machine-id"
	base.BootID = "boot-id"
	base.RuntimeID = "containerd"
	base.SandboxID = "sandbox-id"
	scope, err := json.Marshal(cri.CgroupScope{
		Path: "kubepods.slice/rhiza", Dev: 1, Ino: 2, PID: 3, StartTime: 4,
		RuntimeHash: fencing.RuntimeScopeBindingDigest(base), ProfileID: "test-profile",
	})
	return base, scope, err
}

func (supervisorRegistrationAgentBackend) Stop(context.Context, fencing.RuntimeBinding, fencing.StopRequest, json.RawMessage) (json.RawMessage, error) {
	return nil, fmt.Errorf("stop is not used by registration test")
}

func (supervisorRegistrationAgentBackend) ResumeStop(context.Context, fencing.RuntimeBinding, fencing.StopRequest, json.RawMessage) (json.RawMessage, error) {
	return nil, fmt.Errorf("resume is not used by registration test")
}

type supervisorRegistrationFixture struct {
	e        *FencingExecutor
	state    *AuthorityStateStore
	identity fencing.Identity
	token    string
	server   *httptest.Server
}

func newSupervisorRegistrationFixture(t *testing.T, required bool) supervisorRegistrationFixture {
	t.Helper()
	apiState := &mutationStoreFixture{objects: make(map[string]object)}
	identity := fencing.Identity{SourceClusterID: "source", NodeID: "n0", PodName: "n0", PodUID: "pod-0", RunID: "run-0"}
	env := []any{
		object{"name": "RHIZA_CLUSTER_ID", "value": identity.SourceClusterID},
		object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "gcs"},
		object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "archive"},
		object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"},
	}
	pod := supervisedTestPod()
	metadata := asObject(pod["metadata"])
	metadata["name"], metadata["namespace"], metadata["uid"] = identity.NodeID, "test", identity.PodUID
	metadata["annotations"] = object{fencingProtocolAnnotation: "1", fencingRootAnnotation: "binding-uid"}
	metadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": "sts-uid", "controller": true}}
	spec := asObject(pod["spec"])
	spec["nodeName"], spec["serviceAccountName"] = "node", "default"
	container := asObject(list(spec["containers"])[0])
	container["name"], container["env"] = "rhiza", env
	pod["status"] = object{"containerStatuses": []any{object{
		"name": "rhiza", "containerID": "containerd://0123456789abcdef0123456789abcdef",
		"imageID": "registry.example/rhiza@sha256:" + strings.Repeat("a", 64),
	}}}
	statefulSet := object{
		"metadata": object{"name": "rhiza", "namespace": "test", "uid": "sts-uid"},
		"spec":     object{"template": object{"spec": object{"containers": []any{object{"name": "rhiza", "env": env}}}}},
	}
	apiState.authPod, apiState.authStatefulSet = pod, statefulSet
	apiState.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{statefulSet}}
	apiServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + " " + r.URL.Path {
		case "GET /apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizaclusters":
			root := ClusterResource{Metadata: object{"name": "cluster", "namespace": "test", "uid": "binding-uid"}, Spec: ClusterSpec{
				LogicalID: "logical", StatefulSet: "rhiza", AdoptClusterID: identity.SourceClusterID,
			}}
			_ = json.NewEncoder(w).Encode(object{"items": []any{root}})
			return
		case "GET /api/v1/nodes/node":
			_ = json.NewEncoder(w).Encode(object{"status": object{"addresses": []any{object{"type": "InternalIP", "address": "127.0.0.1"}}}})
			return
		case "GET /api/v1/namespaces/test":
			_ = json.NewEncoder(w).Encode(object{"metadata": object{"name": "test", "uid": "namespace-uid"}})
			return
		}
		apiState.serve(w, r)
	}))
	t.Cleanup(apiServer.Close)
	tokenFile := filepath.Join(t.TempDir(), "authority-token")
	if err := os.WriteFile(tokenFile, []byte("test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	stateBucket := objstore.NewInMemBucket()
	t.Cleanup(func() { _ = stateBucket.Close() })
	state, err := NewAuthorityStateStore(stateBucket, "test-authority")
	if err != nil {
		t.Fatal(err)
	}
	e := &FencingExecutor{
		Kube:           &Kubernetes{BaseURL: apiServer.URL, Namespace: "test", TokenFile: tokenFile, Client: apiServer.Client()},
		AuthorityState: state, ArchiveBucket: stateBucket, RequireMutationJournal: required,
		MutationProfile: fencing.MutationHeldExecutionProfile,
	}
	if required {
		runtimeStateDir := filepath.Join(t.TempDir(), "runtime-agent-state")
		if err := os.Mkdir(runtimeStateDir, 0700); err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		runtimeService, err := runtimeagent.New(runtimeagent.Config{
			AuthorityID: "authority", AuthorityNamespace: "test", AuthorityServiceAccount: "runtime-agent",
			AuthorityWorkloadUID: "runtime-agent-workload", Audience: "rhiza-runtime-agent", Directory: runtimeStateDir,
			Auth: supervisorRegistrationAgentAuth{}, Backend: supervisorRegistrationAgentBackend{},
		})
		if err != nil {
			_ = listener.Close()
			t.Fatal(err)
		}
		runtimeServer := httptest.NewUnstartedServer(runtimeService.Handler())
		runtimeServer.Listener = listener
		runtimeServer.StartTLS()
		t.Cleanup(runtimeServer.Close)
		cert := runtimeServer.Certificate()
		serverName := ""
		if len(cert.DNSNames) != 0 {
			serverName = cert.DNSNames[0]
		} else if len(cert.IPAddresses) != 0 {
			serverName = cert.IPAddresses[0].String()
		}
		if serverName == "" {
			t.Fatal("runtime-agent test certificate has no verifiable SAN")
		}
		ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
		e.RuntimeAgent = &RuntimeAgentClient{Kube: e.Kube, AuthorityID: "authority", TokenFile: tokenFile, CA: ca, ServerName: serverName,
			Port: strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)}
	}
	// The supervisor client talks to the real executor handler. Kubernetes
	// discovery/authentication performed by that handler still uses apiServer.
	authorityServer := httptest.NewTLSServer(e.Handler())
	t.Cleanup(authorityServer.Close)
	return supervisorRegistrationFixture{e: e, state: state, identity: identity, token: tokenFile, server: authorityServer}
}

func TestRequiredMutationProducerIsReservedByAuthenticatedRegistrationBeforeWALBind(t *testing.T) {
	f := newSupervisorRegistrationFixture(t, true)
	ctx := t.Context()
	client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
	if err := client.Register(ctx, f.identity); err != nil {
		t.Fatalf("authenticated supervisor registration did not initialize/enroll/reserve: %v", err)
	}

	rootBeforeBind, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	run := rootBeforeBind.Ledger.Runs[f.identity.RunID]
	if run.Runtime == nil || run.MutationProducerUID == "" || run.Identity.WALIdentity != "" || rootBeforeBind.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisAwaitingRef || rootBeforeBind.Ledger.ArchiveScopeHash == "" {
		t.Fatalf("registration did not enroll runtime and pin a producer before WAL binding: %+v", run)
	}
	producerBeforeBind, producerUID, _, err := f.state.ReadProducer(ctx, *run.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if producerUID != run.MutationProducerUID || producerBeforeBind.Phase != fencing.MutationReserved || producerBeforeBind.WALIdentity != "" || producerBeforeBind.LastAdmitted != 0 || len(producerBeforeBind.Entries) != 0 {
		t.Fatalf("registration producer is not an unused reservation: uid=%q run=%+v producer=%+v", producerUID, run, producerBeforeBind)
	}

	// Registration retries may refresh the root CAS, but must not create a new
	// producer incarnation or add producer history before the supervisor binds WAL.
	if err := client.Register(ctx, f.identity); err != nil {
		t.Fatalf("same-incarnation registration retry: %v", err)
	}
	rootAfterRetry, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	runAfterRetry := rootAfterRetry.Ledger.Runs[f.identity.RunID]
	producerAfterRetry, uidAfterRetry, _, err := f.state.ReadProducer(ctx, *runAfterRetry.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if runAfterRetry.MutationProducerUID != producerUID || uidAfterRetry != producerUID || hashJSON(producerBeforeBind) != hashJSON(producerAfterRetry) || producerAfterRetry.Phase != fencing.MutationReserved || producerAfterRetry.WALIdentity != "" || producerAfterRetry.LastAdmitted != 0 || len(producerAfterRetry.Entries) != 0 {
		t.Fatalf("same-tuple registration retry changed the producer reservation: run=%+v producer=%+v", runAfterRetry, producerAfterRetry)
	}

	walID := f.identity
	walID.WALIdentity = "wal-0"
	receipt, err := client.Bind(ctx, walID)
	if err != nil {
		t.Fatalf("authenticated supervisor WAL bind: %v", err)
	}
	if receipt.Producer == nil || receipt.Producer.ProducerUID != producerUID || receipt.Producer.Runtime != *runAfterRetry.Runtime || receipt.Producer.ExecutionProfile != fencing.MutationHeldExecutionProfile || receipt.Producer.StoreScopeHash == "" || receipt.Producer.StageIncarnation == "" {
		t.Fatalf("WAL bind did not return the previously reserved and scoped producer: %+v", receipt)
	}
	rootAfterBind, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
	if err != nil {
		t.Fatal(err)
	}
	boundRun := rootAfterBind.Ledger.Runs[f.identity.RunID]
	producerAfterBind, uidAfterBind, _, err := f.state.ReadProducer(ctx, *boundRun.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	if boundRun.Identity != walID || boundRun.MutationProducerUID != producerUID || boundRun.MutationScopeHash != receipt.Producer.StoreScopeHash || boundRun.MutationStageIncarnation != receipt.Producer.StageIncarnation || uidAfterBind != producerUID || producerAfterBind.Phase != fencing.MutationOpen || producerAfterBind.WALIdentity != walID.WALIdentity {
		t.Fatalf("WAL bind did not activate the exact reserved producer: run=%+v producer=%+v", boundRun, producerAfterBind)
	}
}

func TestRegistrationProfileRejectionCannotCreateProducerAndCooperativeBindStaysNil(t *testing.T) {
	t.Run("cooperative root rejects required registration without producer", func(t *testing.T) {
		f := newSupervisorRegistrationFixture(t, false)
		ctx := t.Context()
		if err := f.e.initializeRoot(ctx, "sts-uid", f.identity.SourceClusterID); err != nil {
			t.Fatal(err)
		}
		f.e.RequireMutationJournal = true
		client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
		if err := client.Register(ctx, f.identity); err == nil {
			t.Fatal("required executor upgraded an existing cooperative root")
		}
		root, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}
		if root.Ledger.RequireMutationJournal || len(root.Ledger.Runs) != 0 {
			t.Fatalf("failed profile upgrade changed the cooperative root: %+v", root.Ledger)
		}
		// The non-journal profile remains usable through the same authenticated
		// HTTP contract, and never receives a producer binding.
		f.e.RequireMutationJournal = false
		if err := client.Register(ctx, f.identity); err != nil {
			t.Fatalf("cooperative registration: %v", err)
		}
		wal := f.identity
		wal.WALIdentity = "wal-cooperative"
		receipt, err := client.Bind(ctx, wal)
		if err != nil || receipt.Producer != nil {
			t.Fatalf("cooperative bind must remain valid without a mutation producer: receipt=%+v err=%v", receipt, err)
		}
		root, _, err = f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}
		run, ok := root.Ledger.Runs[f.identity.RunID]
		if !ok || run.Identity != wal || run.Runtime != nil || run.MutationProducerUID != "" {
			t.Fatalf("cooperative bind unexpectedly created mutation authority: %+v", run)
		}
	})

	t.Run("closed required root cannot be reopened by registration", func(t *testing.T) {
		f := newSupervisorRegistrationFixture(t, true)
		ctx := t.Context()
		if err := f.e.initializeRoot(ctx, "sts-uid", f.identity.SourceClusterID); err != nil {
			t.Fatal(err)
		}
		if _, err := f.e.change(ctx, "sts-uid", f.identity.SourceClusterID, func(ledger *fencing.Ledger) error {
			return ledger.Close("closed-operation", strings.Repeat("a", 64))
		}); err != nil {
			t.Fatal(err)
		}
		client := fencing.AuthorityClient{URL: f.server.URL + "/v1/supervisor", TokenFile: f.token, Client: f.server.Client()}
		if err := client.Register(ctx, f.identity); err == nil {
			t.Fatal("registration reopened a closed required root")
		}
		root, _, err := f.state.ReadRoot(ctx, "sts-uid", f.identity.SourceClusterID)
		if err != nil {
			t.Fatal(err)
		}
		if root.Ledger.ClosedBy != "closed-operation" || len(root.Ledger.Runs) != 0 {
			t.Fatalf("closed root changed during rejected registration: %+v", root.Ledger)
		}
		if _, _, _, err := f.state.ReadProducer(ctx, fencing.RuntimeBinding{RunID: f.identity.RunID, StatefulSetUID: "sts-uid", SourceClusterID: f.identity.SourceClusterID}); err == nil {
			t.Fatal("closed-root registration created a mutation producer")
		}
	})
}
