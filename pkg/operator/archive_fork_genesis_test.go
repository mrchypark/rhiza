package operator

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/fencing"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestArchiveForkRegistrationRequiresConfiguredControllerToken(t *testing.T) {
	for _, tc := range []struct {
		name     string
		username string
		want     int
	}{
		{name: "controller principal", username: "system:serviceaccount:test:rhiza-operator", want: http.StatusConflict},
		{name: "source workload principal", username: "system:serviceaccount:test:rhiza-fencer", want: http.StatusForbidden},
		{name: "other controller", username: "system:serviceaccount:test:other", want: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/apis/authentication.k8s.io/v1/tokenreviews" || r.Method != http.MethodPost {
					http.NotFound(w, r)
					return
				}
				var review object
				_ = json.NewDecoder(r.Body).Decode(&review)
				audiences := list(nested(review, "spec", "audiences"))
				if len(audiences) != 1 || str(audiences[0]) != "rhiza-fencer" {
					t.Errorf("TokenReview audience = %v, want rhiza-fencer", audiences)
				}
				_ = json.NewEncoder(w).Encode(object{"status": object{
					"authenticated": true,
					"audiences":     []string{"rhiza-fencer"},
					"user":          object{"username": tc.username},
				}})
			}))
			defer api.Close()
			token := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(token, []byte("test-token"), 0600); err != nil {
				t.Fatal(err)
			}
			e := &FencingExecutor{
				Kube:                     &Kubernetes{BaseURL: api.URL, Namespace: "test", TokenFile: token, Client: api.Client()},
				ControllerServiceAccount: "rhiza-operator",
			}
			body := `{"statefulSetUID":"sts-uid","targetClusterID":"target","operationID":"recovery-op","sourceFenceOperationID":"fence-op"}`
			req := httptest.NewRequest(http.MethodPost, "/v1/archive-genesis/fork/register", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer test-token")
			recorder := httptest.NewRecorder()
			e.Handler().ServeHTTP(recorder, req)
			if recorder.Code != tc.want {
				t.Fatalf("HTTP status = %d, want %d; body=%q", recorder.Code, tc.want, recorder.Body.String())
			}
		})
	}
}

type forkAuthorityRegistrarFixture struct {
	e       *FencingExecutor
	kube    *mutationStoreFixture
	runtime fencing.RuntimeBinding
	id      fencing.Identity
	request ArchiveForkGenesisRequest
	scope   fencing.ArchiveScope
	ref     fencing.ArchiveGenesisRef
}

func newForkAuthorityRegistrarFixture(t *testing.T) forkAuthorityRegistrarFixture {
	t.Helper()
	f := newArchiveGenesisFixture(t)
	e, bucket := f.e, f.e.ArchiveBucket
	e.RequireMutationJournal = true

	sourceMembers := []quepaxa.Member{forkTestMember(t, "source-voter")}
	targetMembers := []quepaxa.Member{forkTestMember(t, "target-voter")}
	container := list(nested(f.f.authStatefulSet, "spec", "template", "spec", "containers"))[0]
	env := list(asObject(container)["env"])
	env = append(env,
		object{"name": "RHIZA_OBJSTORE_DURABILITY", "value": "async"},
		object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(jsonBytes(sourceMembers))},
	)
	asObject(container)["env"] = env
	f.f.mu.Lock()
	f.f.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{f.f.authStatefulSet}}
	f.f.objects["/api/v1/namespaces/test/pods"] = object{"items": []any{}}
	f.f.mu.Unlock()

	const sourcePrefix, targetID, recoveryOperation, fenceOperation = "root/source", "target", "recovery-op", "source-fence-op"
	putForkJSON(t, bucket, path.Join(sourcePrefix, "voters/membership.json"), recovery.NewMembershipRecord("source", sourceMembers, "async"))
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: sourceMembers[0].ID, Cluster: quepaxa.Cluster{ConfigID: recovery.ArchiveConfigID, Members: sourceMembers}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := core.Propose(t.Context(), []byte("fork registrar source decision")); err != nil {
		t.Fatal(err)
	}
	archive := recovery.NewManager(bucket, sourcePrefix, recovery.ArchiveConfigID)
	if err := archive.SyncThrough(t.Context(), core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Seal(t.Context(), bucket, sourcePrefix, fenceOperation); err != nil {
		t.Fatal(err)
	}
	archive.Close()
	targetPrefix := path.Join("root", targetID)
	targetMembership := recovery.NewMembershipRecord(targetID, targetMembers, "async")
	result, err := recovery.Fork(t.Context(), bucket, recovery.ForkOptions{
		SourcePrefix: sourcePrefix, TargetPrefix: targetPrefix,
		SourceBootstrap: quepaxa.Cluster{ConfigID: recovery.ArchiveConfigID, Members: sourceMembers},
		TargetMembers:   targetMembers, TargetMembership: targetMembership, OperationID: recoveryOperation,
	})
	if err != nil {
		t.Fatal(err)
	}
	putForkJSON(t, bucket, path.Join(targetPrefix, "voters/membership.json"), targetMembership)
	lineage := archiveForkLineageRecord{Version: 1, OperationID: recoveryOperation, Source: "source", SourceDurability: "async",
		SourceMembership: sourceFingerprint("source", "async", sourceMembers, sourcePrefix), TargetMembership: targetMembership, Fork: result}
	putForkJSON(t, bucket, path.Join(targetPrefix, "recovery/generation.json"), lineage)
	reservation := archiveForkReservation{Version: 1, OperationID: recoveryOperation, Target: targetID, Source: "source",
		SourceMembership: sourceFingerprint("source", "async", sourceMembers, sourcePrefix), StatefulSetUID: f.runtime.StatefulSetUID, SpecHash: strings.Repeat("a", 64)}
	putForkJSON(t, bucket, path.Join(sourcePrefix, "recovery/successor.json"), reservation)

	fenceRequest := FenceRequest{LogicalID: "fork-test", BindingUID: f.runtime.BindingUID, Namespace: "test", StatefulSet: "rhiza",
		StatefulSetUID: f.runtime.StatefulSetUID, SourceClusterID: "source", OperationID: fenceOperation, Scope: ScopeGeneration}
	fenceHash, err := requestHash(&fenceRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.change(t.Context(), f.runtime.StatefulSetUID, "source", func(l *fencing.Ledger) error { return l.Close(fenceOperation, fenceHash) }); err != nil {
		t.Fatal(err)
	}
	e.CollectRuntimeEvidence = func(context.Context, fencing.RuntimeBinding, fencing.StopRequest) ([]byte, error) {
		return []byte("registrar-test-terminal"), nil
	}
	e.VerifyRuntimeEvidence = func(_ context.Context, _ fencing.RuntimeBinding, _ fencing.StopRequest, receipt []byte) error {
		if string(receipt) != "registrar-test-terminal" {
			return fmt.Errorf("unexpected test terminal receipt")
		}
		return nil
	}
	fenceName := "fence-" + hashJSON([]string{f.runtime.BindingUID, fenceOperation})[:40]
	fence := &fenceResource{APIVersion: "rhiza.mrchypark.dev/v1alpha1", Kind: "RhizaFence",
		Metadata: object{"name": fenceName, "namespace": "test", "uid": "fence-uid", "resourceVersion": "1"}, Spec: fenceRequest}
	if err := e.reconcileFence(t.Context(), e.controller().corePath("rhizafences", ""), fence); err != nil {
		t.Fatal(err)
	}
	fencePath := "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences/" + fenceName
	fenceData, _ := json.Marshal(fence)
	var fenceObject object
	if err := json.Unmarshal(fenceData, &fenceObject); err != nil {
		t.Fatal(err)
	}
	f.f.mu.Lock()
	f.f.objects[fencePath] = fenceObject
	f.f.objects["/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/test/rhizafences"] = object{"items": []any{fenceObject}}
	f.f.mu.Unlock()

	request := ArchiveForkGenesisRequest{StatefulSetUID: f.runtime.StatefulSetUID, TargetClusterID: targetID,
		OperationID: recoveryOperation, SourceFenceOperationID: fenceOperation}
	scope := fencing.ArchiveScope{Provider: "gcs", Bucket: "archive", Prefix: targetPrefix, ClusterID: targetID}
	ref, err := e.verifyArchiveForkArtifacts(t.Context(), request, "source", sourcePrefix, targetPrefix, scope, sourceMembers, "async")
	if err != nil {
		t.Fatal(err)
	}
	return forkAuthorityRegistrarFixture{e: e, kube: f.f, runtime: f.runtime, id: f.id, request: request, scope: scope, ref: ref}
}

func forkTestMember(t *testing.T, id string) quepaxa.Member {
	t.Helper()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var key quepaxa.PublicKey
	copy(key[:], public)
	return quepaxa.Member{ID: quepaxa.NodeID(id), PublicKey: key}
}

func putForkJSON(t *testing.T, bucket objstore.Bucket, key string, value any) {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Upload(t.Context(), key, bytes.NewReader(body)); err != nil {
		t.Fatal(err)
	}
}

func TestArchiveForkRegistrarCommitsVerifiedForkAndReportsProfile(t *testing.T) {
	f := newForkAuthorityRegistrarFixture(t)
	response, err := f.e.registerArchiveForkGenesis(t.Context(), f.request)
	if err != nil || response != (ArchiveForkGenesisResponse{Profile: "independent", GenesisRegistered: true}) {
		t.Fatalf("registration response=%+v err=%v", response, err)
	}
	root, err := f.e.read(t.Context(), f.request.StatefulSetUID, f.request.TargetClusterID)
	if err != nil || root.Ledger.ArchiveGenesisState != fencing.ArchiveGenesisCommitted || root.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, &f.ref) {
		t.Fatalf("target root did not commit the verified fork reference: %+v err=%v", root.Ledger, err)
	}
	claim, _, err := f.e.AuthorityState.ReadArchiveScopeClaim(t.Context(), f.scope)
	if err != nil || claim.GenesisRef == nil || !sameArchiveGenesisRef(claim.GenesisRef, &f.ref) {
		t.Fatalf("target scope claim does not bind verified fork: %+v err=%v", claim, err)
	}
	// The target workload's first actual runtime registration must join the
	// controller-created root and immutable claim without replacing either.
	f.kube.mu.Lock()
	sts := f.kube.authStatefulSet
	containers := list(nested(sts, "spec", "template", "spec", "containers"))
	env := list(asObject(containers[0])["env"])
	for _, entry := range env {
		if str(asObject(entry)["name"]) == "RHIZA_CLUSTER_ID" {
			asObject(entry)["value"] = f.request.TargetClusterID
		}
	}
	f.kube.authStatefulSet = sts
	f.kube.objects["/apis/apps/v1/namespaces/test/statefulsets"] = object{"items": []any{sts}}
	f.kube.mu.Unlock()
	targetID := fencing.Identity{SourceClusterID: f.request.TargetClusterID, NodeID: "target-voter", PodUID: "target-pod", RunID: "target-run"}
	targetRuntime := f.runtime
	targetRuntime.SourceClusterID = f.request.TargetClusterID
	targetRuntime.NodeID, targetRuntime.PodUID, targetRuntime.RunID = targetID.NodeID, targetID.PodUID, targetID.RunID
	targetRuntime.BindingUID, targetRuntime.NodeUID = "target-binding", "target-node-uid"
	targetRuntime.RuntimeID, targetRuntime.SandboxID, targetRuntime.ContainerID = "target-runtime", "target-sandbox", "target-container"
	targetRuntime.ImageID = "target-image"
	if _, err := f.e.change(t.Context(), f.request.StatefulSetUID, targetID.SourceClusterID, func(l *fencing.Ledger) error {
		return l.RegisterEnrolled(targetID, targetRuntime)
	}); err != nil {
		t.Fatalf("target runtime registration could not join committed genesis: %v", err)
	}
	if err := f.e.claimArchiveScopeForRegistration(t.Context(), f.request.StatefulSetUID, targetID, targetRuntime); err != nil {
		t.Fatalf("target registration rejected the controller-created scope claim: %v", err)
	}
	producer, err := f.e.reserveMutationProducer(t.Context(), targetRuntime)
	if err != nil || producer.Phase != fencing.MutationReserved {
		t.Fatalf("target producer reservation=%+v err=%v", producer, err)
	}
	targetID.WALIdentity = "target-wal"
	f.kube.mu.Lock()
	stsSpec := nested(f.kube.authStatefulSet, "spec", "template", "spec")
	f.kube.objects["/api/v1/namespaces/test/pods/"+targetID.NodeID] = object{
		"metadata": object{"name": targetID.NodeID, "uid": targetID.PodUID, "ownerReferences": []any{object{"kind": "StatefulSet", "name": "rhiza", "uid": f.request.StatefulSetUID}}},
		"spec":     stsSpec,
	}
	f.kube.mu.Unlock()
	receipt, err := f.e.bindSupervisorWAL(t.Context(), f.request.StatefulSetUID, targetID)
	if err != nil || receipt.Producer == nil || receipt.Producer.Runtime != targetRuntime {
		t.Fatalf("target WAL bind rejected the verified fork root: receipt=%+v err=%v", receipt, err)
	}
	root, err = f.e.read(t.Context(), f.request.StatefulSetUID, f.request.TargetClusterID)
	if err != nil || root.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, &f.ref) {
		t.Fatalf("target registration changed immutable genesis: root=%+v err=%v", root.Ledger, err)
	}
}

func TestArchiveForkRegistrarRejectsMismatchesAndMissingArtifacts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*forkAuthorityRegistrarFixture)
	}{
		{name: "recovery operation mismatch", change: func(f *forkAuthorityRegistrarFixture) { f.request.OperationID = "other-recovery" }},
		{name: "source fence mismatch", change: func(f *forkAuthorityRegistrarFixture) { f.request.SourceFenceOperationID = "other-fence" }},
		{name: "missing fork result", change: func(f *forkAuthorityRegistrarFixture) {
			_ = f.e.ArchiveBucket.Delete(t.Context(), "root/target/fork/result.json")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newForkAuthorityRegistrarFixture(t)
			tc.change(&f)
			if _, err := f.e.registerArchiveForkGenesis(t.Context(), f.request); err == nil {
				t.Fatal("mismatched or incomplete fork registration succeeded")
			}
		})
	}
}

func TestArchiveForkRegistrarCompletesPartialScopeReferenceCAS(t *testing.T) {
	f := newForkAuthorityRegistrarFixture(t)
	if err := f.e.initializeRoot(t.Context(), f.request.StatefulSetUID, f.request.TargetClusterID); err != nil {
		t.Fatal(err)
	}
	runtime := fencing.RuntimeBinding{StatefulSetUID: f.request.StatefulSetUID, SourceClusterID: f.request.TargetClusterID}
	if err := f.e.claimArchiveScopeForRegistration(t.Context(), f.request.StatefulSetUID, fencing.Identity{SourceClusterID: f.request.TargetClusterID}, runtime); err != nil {
		t.Fatal(err)
	}
	if err := f.e.AuthorityState.SetArchiveScopeGenesisRef(t.Context(), f.scope, f.ref); err != nil {
		t.Fatal(err)
	}
	response, err := f.e.registerArchiveForkGenesis(t.Context(), f.request)
	if err != nil || response != (ArchiveForkGenesisResponse{Profile: "independent", GenesisRegistered: true}) {
		t.Fatalf("partial reference retry response=%+v err=%v", response, err)
	}
	root, err := f.e.read(t.Context(), f.request.StatefulSetUID, f.request.TargetClusterID)
	if err != nil || root.Ledger.ArchiveGenesisRef == nil || !sameArchiveGenesisRef(root.Ledger.ArchiveGenesisRef, &f.ref) {
		t.Fatalf("partial CAS retry did not repair exact target root ref: %+v err=%v", root.Ledger, err)
	}
}
