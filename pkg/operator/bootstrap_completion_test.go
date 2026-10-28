package operator

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

type bootstrapCompletionAPI struct {
	mu      sync.Mutex
	root    ClusterResource
	objects map[string]object
	puts    int
	calls   []string
}

func (a *bootstrapCompletionAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, r.Method+" "+r.URL.Path)
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path == (&Controller{Kube: &Kubernetes{Namespace: "default"}}).clusterPath("") {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []ClusterResource{a.root}})
		return
	}
	if r.Method == http.MethodPut {
		a.puts++
		var value object
		if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if _, ok := a.objects[r.URL.Path]; !ok {
			http.NotFound(w, r)
			return
		}
		a.objects[r.URL.Path] = value
		_ = json.NewEncoder(w).Encode(value)
		return
	}
	value, ok := a.objects[r.URL.Path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	_ = json.NewEncoder(w).Encode(value)
}

func TestCompleteBootstrapRequiresPinnedCertifiedBarrierBeforeLedgerCAS(t *testing.T) {
	for _, scenario := range []string{"valid", "wrong-nonce", "missing-extent", "changed-credential-uid"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			bucket := objstore.NewInMemBucket()
			member := quepaxa.Member{ID: "rhiza-0", PublicKey: quepaxa.PublicKey(sha256.Sum256([]byte("member-key")))}
			members := []quepaxa.Member{member}
			initial := quepaxa.Cluster{ConfigID: 1, Members: members}
			operationID := "bootstrap-operation"
			var sourceNonce [quepaxa.ReadBarrierNonceSize]byte

			wal, err := qlog.Open(path.Join(t.TempDir(), "source-wal"))
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			source, err := quepaxa.New(quepaxa.Config{NodeID: member.ID, Cluster: initial, WAL: wal, EnableReconfiguration: true})
			if err != nil {
				t.Fatal(err)
			}
			archivePrefix := path.Join("prefix", "source")
			if err := uploadBootstrapTestObject(ctx, bucket, path.Join(archivePrefix, "voters", "membership.json"), recovery.NewMembershipRecord("source", members, "before-ack")); err != nil {
				t.Fatal(err)
			}
			writer := recovery.NewManager(bucket, archivePrefix, 1)
			defer writer.Close()

			root := ClusterResource{
				Metadata: object{"name": "rhiza", "uid": "root-uid"},
				Spec:     ClusterSpec{LogicalID: "logical", StatefulSet: "rhiza", Container: "rhiza", AdoptClusterID: "source", VoterCount: 1, Automatic: true},
			}
			normalizeClusterSpec(&root)
			stsUID := "sts-uid"
			operationCandidate := expectedBootstrapCandidate(&root, object{
				"metadata": object{"uid": stsUID},
				"spec":     object{"serviceName": "rhiza-headless"},
			})
			operationID = bootstrapOperationID("root-uid", stsUID, operationCandidate)
			sourceNonce = fencing.BootstrapBarrierNonce(operationID)
			if scenario == "wrong-nonce" {
				sourceNonce[0] ^= 0xff
			}
			if _, _, err := source.Propose(ctx, quepaxa.EncodeReadBarrier(sourceNonce)); err != nil {
				t.Fatal(err)
			}
			if err := writer.SyncThrough(ctx, source, source.Tip()); err != nil {
				t.Fatal(err)
			}

			pod := prepareProfilePod()
			podUID, podName := "pod-uid", "rhiza-0"
			podMetadata := asObject(pod["metadata"])
			podMetadata["name"], podMetadata["uid"] = podName, podUID
			annotations := asObject(podMetadata["annotations"])
			annotations[fencingRootAnnotation] = "root-uid"
			annotations[fencingPrepareOperationAnnotation] = operationID
			podMetadata["ownerReferences"] = []any{object{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "rhiza", "uid": stsUID, "controller": true}}
			pod["status"] = object{"initContainerStatuses": []any{object{"name": prepareInitContainerName, "state": object{"terminated": object{"containerID": "containerd://init", "imageID": "sha256:image", "exitCode": float64(0), "reason": "Completed"}}}}}
			podSpec := asObject(pod["spec"])
			sts := object{
				"metadata": object{"name": "rhiza", "uid": stsUID},
				"spec": object{
					"podManagementPolicy": "Parallel", "serviceName": "rhiza-headless",
					"template": object{"metadata": object{"annotations": object{fencingRootAnnotation: "root-uid", fencingPrepareOperationAnnotation: operationID}}, "spec": podSpec},
				},
			}
			for _, containerName := range []string{"rhiza"} {
				ctr := asObject(list(nested(podSpec, "containers"))[0])
				if ctr["name"] != containerName {
					t.Fatalf("unexpected fixture container %v", ctr["name"])
				}
			}
			for _, entry := range list(nested(podSpec, "containers")) {
				setBootstrapEnv(asObject(entry), "RHIZA_CLUSTER_ID", "source")
				setBootstrapEnv(asObject(entry), "RHIZA_OBJSTORE_BUCKET", "test-bucket")
				setBootstrapEnv(asObject(entry), "RHIZA_OBJSTORE_PREFIX", "prefix")
				setBootstrapEnv(asObject(entry), "RHIZA_OBJSTORE_DURABILITY", "before-ack")
				setBootstrapEnv(asObject(entry), "RHIZA_ENABLE_RECONFIGURATION", "true")
			}
			for _, entry := range list(nested(podSpec, "initContainers")) {
				setBootstrapEnv(asObject(entry), "RHIZA_CLUSTER_ID", "source")
				setBootstrapEnv(asObject(entry), "RHIZA_OBJSTORE_BUCKET", "test-bucket")
				setBootstrapEnv(asObject(entry), "RHIZA_OBJSTORE_PREFIX", "prefix")
				setBootstrapEnv(asObject(entry), "RHIZA_OBJSTORE_DURABILITY", "before-ack")
				setBootstrapEnv(asObject(entry), "RHIZA_ENABLE_RECONFIGURATION", "true")
			}

			profileHash := prepareProfileHash(sts)
			membersBytes := jsonBytes(members)
			credentialUID, credentialName := "credential-uid", "cluster-credentials"
			owner := &Resource{Metadata: object{"uid": "root-uid"}, Spec: Spec{RecoveryID: "logical"}, Status: Status{Target: "source"}}
			secret := object{
				"metadata":  object{"name": credentialName, "namespace": "default", "uid": credentialUID, "annotations": object{ownerAnnotation: (&Controller{}).operationID(owner)}},
				"immutable": true,
				"data":      object{"members": base64.StdEncoding.EncodeToString(membersBytes)},
			}
			assignment := fencing.BootstrapAssignment{
				OperationID: operationID, RequestHash: hashJSON(operationCandidate),
				Purpose: fencing.BootstrapInitial, RootUID: "root-uid", StatefulSetUID: stsUID, SourceClusterID: "source",
				PodName: podName, PodUID: podUID, NodeID: string(member.ID), CredentialName: credentialName, CredentialUID: credentialUID,
				MembershipHash: hashJSON(members), ProfileHash: profileHash, LedgerUID: "ledger-uid",
			}
			authorizationHash := hashJSON(struct{ RootUID, StatefulSetUID, SourceClusterID, CredentialName, CredentialUID, MembershipHash, ProfileHash string }{
				"root-uid", stsUID, "source", credentialName, credentialUID, assignment.MembershipHash, profileHash,
			})
			assignment.AuthorizationHash = authorizationHash
			pin := bootstrapLedgerPin{
				Version: 1, OperationID: operationID, RequestHash: assignment.RequestHash, AuthorizationHash: authorizationHash,
				RootUID: "root-uid", StatefulSetUID: stsUID, SourceClusterID: "source", CredentialName: credentialName,
				CredentialUID: credentialUID, MembershipHash: assignment.MembershipHash, ProfileHash: profileHash, LedgerUID: "ledger-uid",
			}
			controller := &Controller{Kube: &Kubernetes{Namespace: "default"}, Bucket: bucket, Prefix: "prefix"}
			if err := uploadBootstrapTestObject(ctx, bucket, controller.bootstrapCandidateKey(&root), operationCandidate); err != nil {
				t.Fatal(err)
			}
			if err := uploadBootstrapTestObject(ctx, bucket, controller.bootstrapLedgerPinKey(&root), pin); err != nil {
				t.Fatal(err)
			}

			ledger := fencing.Ledger{SourceClusterID: "source"}
			operation := fencing.BootstrapOperation{
				OperationID: operationID, RequestHash: assignment.RequestHash, AuthorizationHash: authorizationHash,
				Purpose: fencing.BootstrapInitial, RootUID: "root-uid", StatefulSetUID: stsUID, SourceClusterID: "source",
			}
			if err := ledger.InstallBootstrapOperation(operation); err != nil {
				t.Fatal(err)
			}
			if err := ledger.AssignBootstrap(assignment); err != nil {
				t.Fatal(err)
			}
			if _, err := ledger.BeginInit(assignment, "attempt-1", fencing.InitExecutionScope{PodName: podName, PodUID: podUID, ContainerName: prepareInitContainerName, ContainerID: "containerd://init", ImageID: "sha256:image", NodeName: "worker", NodeUID: "node-uid"}); err != nil {
				t.Fatal(err)
			}
			outcome := fencing.InitAttemptOutcome{State: fencing.InitAttemptMaterialized, GenesisDigest: fencing.BootstrapAssignmentDigest(assignment), LiveCleanup: fencing.InitLiveCleanup{Mode: fencing.InitModeGenesis, CleanupSucceeded: true, NamespaceQuiescent: true}}
			if err := ledger.RecordInitOutcome(assignment, "attempt-1", outcome); err != nil {
				t.Fatal(err)
			}
			if err := ledger.RecordInitTerminal(assignment, "attempt-1", fencing.InitTerminalEvidence{ContainerID: "containerd://init", ImageID: "sha256:image", ExitCode: 0, Reason: "Completed"}); err != nil {
				t.Fatal(err)
			}
			runtime := fencing.Identity{SourceClusterID: "source", NodeID: string(member.ID), PodUID: podUID, RunID: "runtime-1", WALIdentity: "wal-1"}
			registered := runtime
			registered.WALIdentity = ""
			if err := ledger.Register(registered); err != nil {
				t.Fatal(err)
			}
			if err := ledger.Bind(runtime); err != nil {
				t.Fatal(err)
			}
			ledgerPath := (&FencingExecutor{Kube: controller.Kube}).ledgerPath(stsUID, "source")
			ledgerObject := object{
				"apiVersion": "v1", "kind": "ConfigMap",
				"metadata": object{"name": path.Base(ledgerPath), "namespace": "default", "uid": "ledger-uid", "resourceVersion": "1"},
				"data":     object{"ledger.json": string(jsonBytes(fenceLedger{StatefulSetUID: stsUID, Ledger: ledger}))},
			}
			apiState := &bootstrapCompletionAPI{root: root, objects: map[string]object{
				controller.stsPath("rhiza"):                    sts,
				controller.corePath("pods", podName):           pod,
				controller.corePath("secrets", credentialName): secret,
				ledgerPath: ledgerObject,
			}}
			server := httptest.NewTLSServer(apiState)
			defer server.Close()
			tokenFile := path.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("test-token"), 0600); err != nil {
				t.Fatal(err)
			}
			kube := &Kubernetes{BaseURL: server.URL, Namespace: "default", TokenFile: tokenFile, Client: server.Client()}
			location := map[string]string{"RHIZA_OBJSTORE_PROVIDER": "s3", "RHIZA_OBJSTORE_BUCKET": "test-bucket", "RHIZA_OBJSTORE_PREFIX": "prefix"}
			executor := &FencingExecutor{Kube: kube, Bucket: bucket, Prefix: "prefix", StoreIdentity: StoreIdentity(location)}
			completion := fencing.BootstrapCompletion{OperationID: operationID, AuthorizationHash: authorizationHash, AssignmentSetHash: fencing.BootstrapAssignmentSetHash([]fencing.BootstrapAssignment{assignment}), CertifiedBarrierRef: "1"}
			if scenario == "valid" {
				_, actualSTS, actualRoot, err := executor.bootstrapCaller(ctx, podName, runtime, operationID)
				if err != nil {
					t.Fatalf("fixture bootstrap caller: %v", err)
				}
				if actualSTS != stsUID || actualRoot != "root-uid" {
					t.Fatalf("fixture caller identities got StatefulSet=%q root=%q, want %q/%q", actualSTS, actualRoot, stsUID, "root-uid")
				}
			}
			if scenario == "missing-extent" {
				var blocks []string
				if err := bucket.Iter(ctx, path.Join(archivePrefix, "archive", "blocks"), func(name string) error {
					blocks = append(blocks, name)
					return nil
				}); err != nil || len(blocks) == 0 {
					t.Fatalf("list archive extents: blocks=%v err=%v", blocks, err)
				}
				for _, block := range blocks {
					if err := bucket.Delete(ctx, block); err != nil {
						t.Fatal(err)
					}
				}
			}
			if scenario == "changed-credential-uid" {
				apiState.objects[controller.corePath("secrets", credentialName)]["metadata"].(object)["uid"] = "replaced-credential-uid"
			}
			apiState.mu.Lock()
			apiState.calls = nil
			apiState.puts = 0
			apiState.mu.Unlock()
			err = executor.completeBootstrap(ctx, stsUID, runtime, completion)
			if scenario == "valid" {
				if err != nil {
					t.Fatalf("valid certified bootstrap completion: %v (ledger PUTs %d; calls %v)", err, apiState.puts, apiState.calls)
				}
			} else if err == nil {
				t.Fatalf("%s proof/binding was accepted", scenario)
			}
			apiState.mu.Lock()
			stored := apiState.objects[ledgerPath]
			var after fenceLedger
			if err := json.Unmarshal([]byte(str(nested(stored, "data", "ledger.json"))), &after); err != nil {
				apiState.mu.Unlock()
				t.Fatal(err)
			}
			completed := after.Ledger.Bootstrap.Operations[operationID].Complete
			admissionClosed := after.Ledger.Bootstrap.Operations[operationID].AdmissionClosed
			apiState.mu.Unlock()
			if completed != (scenario == "valid") || admissionClosed != (scenario == "valid") {
				t.Fatalf("scenario %s complete=%t admissionClosed=%t", scenario, completed, admissionClosed)
			}
			wantPuts := 0
			if scenario == "valid" {
				wantPuts = 1
			}
			if apiState.puts != wantPuts {
				t.Fatalf("scenario %s performed %d ledger writes", scenario, apiState.puts)
			}
		})
	}
}

func setBootstrapEnv(container object, name, value string) {
	for _, raw := range list(container["env"]) {
		entry := asObject(raw)
		if str(entry["name"]) == name {
			entry["value"] = value
			delete(entry, "valueFrom")
			return
		}
	}
	container["env"] = append(list(container["env"]), object{"name": name, "value": value})
}

func uploadBootstrapTestObject(ctx context.Context, bucket objstore.Bucket, key string, value any) error {
	return bucket.Upload(ctx, key, strings.NewReader(string(jsonBytes(value))))
}
