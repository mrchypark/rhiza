package operator

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/thanos-io/objstore"
)

func TestControllerObservationAndFenceDoNotMutateStatefulSet(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Observed" || api.stsPuts != 0 {
		t.Fatalf("observation=%+v puts=%d", r.Status, api.stsPuts)
	}
	r.Spec.RecoveryID = "recovery-1"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "AwaitingFence" || api.stsPuts != 0 {
		t.Fatalf("fence=%+v puts=%d", r.Status, api.stsPuts)
	}
	if _, err := bucket.Get(ctx, "root/source/recovery/successor.json"); !bucket.IsObjNotFoundErr(err) {
		t.Fatalf("unexpected successor: %v", err)
	}
}

func TestControllerAsyncSourceRequiresLossPolicyForBeforeAckTarget(t *testing.T) {
	ctx, _, r, _, c := controllerFixture(t, "async", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || r.Status.Message != "async source requires explicit allowDataLoss, regardless of target mode" {
		t.Fatalf("status=%+v", r.Status)
	}
}

func TestControllerStagesResumeAfterStatusFailure(t *testing.T) {
	for _, sourceMode := range []string{"before-ack", "async"} {
		t.Run(sourceMode, func(t *testing.T) {
			ctx, _, r, api, c := controllerFixture(t, sourceMode, "before-ack")
			r.Spec.RecoveryID = "recovery-1"
			r.Spec.Fence = fence(r)
			r.Spec.AllowDataLoss = sourceMode == "async"
			advance := func(stage string) {
				t.Helper()
				if err := c.reconcileAll(ctx); err != nil {
					t.Fatal(err)
				}
				if r.Status.Stage != stage {
					t.Fatalf("stage=%q want=%q status=%+v", r.Status.Stage, stage, r.Status)
				}
			}
			advance("Reserved")
			advance("Sealed")
			api.failStatus = 1
			if err := c.reconcileAll(ctx); err == nil {
				t.Fatal("status failure unexpectedly succeeded")
			}
			if api.secret == nil || r.Status.Stage != "Sealed" || api.zeroPuts != 0 {
				t.Fatalf("fork must be resumable before source deletion: secret=%v stage=%q zero=%d", api.secret != nil, r.Status.Stage, api.zeroPuts)
			}
			secret := str(nested(api.secret, "data", "members"))
			advance("Stopping")
			if api.zeroPuts != 0 || str(nested(api.secret, "data", "members")) != secret {
				t.Fatalf("resume changed preserved target: zero=%d", api.zeroPuts)
			}
			advance("Stopping")
			if api.zeroPuts != 1 || number(nested(api.sts, "spec", "replicas")) != 0 {
				t.Fatalf("source not scaled once: zero=%d sts=%v", api.zeroPuts, api.sts)
			}
			advance("Starting")
			advance("Restarted")
			advance("Complete")
			if r.Status.Phase != "Complete" || number(nested(api.sts, "spec", "replicas")) != 3 {
				t.Fatalf("completion=%+v", r.Status)
			}
		})
	}
}

func TestControllerBlocksSuccessorAndDrift(t *testing.T) {
	ctx, bucket, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	if err := bucket.Upload(ctx, "root/source/recovery/successor.json", bytes.NewReader([]byte(`{"operation_id":"other"}`)), objstore.WithIfNotExists()); err != nil {
		t.Fatal(err)
	}
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Phase != "Blocked" || api.stsPuts != 0 {
		t.Fatalf("successor=%+v puts=%d", r.Status, api.stsPuts)
	}

	ctx, _, r, api, c = controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	api.sts["metadata"].(object)["uid"] = "new-uid"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Message != "StatefulSet identity changed during recovery" {
		t.Fatalf("uid drift=%+v", r.Status)
	}

	ctx, _, r, _, c = controllerFixture(t, "before-ack", "before-ack")
	r.Spec.RecoveryID = "recovery-1"
	r.Spec.Fence = fence(r)
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	r.Spec.Container = "other"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Message != "reserved recovery request is immutable; create a new resource" {
		t.Fatalf("spec drift=%+v", r.Status)
	}
}

func TestControllerRejectsUnknownTargetDurability(t *testing.T) {
	ctx, _, r, api, c := controllerFixture(t, "before-ack", "before-ack")
	r.Spec.Durability = "unknown"
	if err := c.reconcileAll(ctx); err != nil {
		t.Fatal(err)
	}
	if r.Status.Message != "target durability must be async or before-ack" || api.stsPuts != 0 {
		t.Fatalf("status=%+v", r.Status)
	}
}

func fence(r *Resource) Fence {
	return Fence{RecoveryID: r.Spec.RecoveryID, ClusterID: r.Spec.SourceClusterID, StatefulSetUID: "sts-uid", Confirmed: true, Evidence: "incident-42"}
}

func controllerFixture(t *testing.T, sourceMode, targetMode string) (context.Context, objstore.Bucket, *Resource, *operatorAPI, *Controller) {
	t.Helper()
	ctx := context.Background()
	bucket := objstore.NewInMemBucket()
	members := members()
	seedArchive(t, ctx, bucket, "root/source", members, sourceMode)
	r := &Resource{Metadata: map[string]any{"name": "recover", "uid": "resource-uid", "generation": float64(1), "resourceVersion": "1"}, Spec: Spec{StatefulSet: "rhiza", SourceClusterID: "source", Durability: targetMode}}
	api := newOperatorAPI(t, r, sourceMode)
	t.Cleanup(api.Close)
	return ctx, bucket, r, api, &Controller{Kube: api.kube(t), Bucket: bucket, Prefix: "root", StoreIdentity: storeIdentity()}
}

type operatorAPI struct {
	mu                            sync.Mutex
	r                             *Resource
	sts                           object
	secret                        object
	failStatus, stsPuts, zeroPuts int
	server                        *httptest.Server
	peers                         []*httptest.Server
}

func newOperatorAPI(t *testing.T, r *Resource, mode string) *operatorAPI {
	a := &operatorAPI{r: r, sts: fakeSTS(mode)}
	for _, member := range members() {
		member := member
		a.peers = append(a.peers, httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, q *http.Request) {
			if q.URL.Path != "/recovery/status" {
				http.NotFound(w, q)
				return
			}
			_ = json.NewEncoder(w).Encode(network.VoterRecoveryStatus{NodeID: string(member.ID), ClusterID: "rhiza-r-" + shortID("resource-uid:recovery-1"), Durability: "before-ack", Ready: true, Quorum: true, CertifiedTip: 1, AppliedTip: 1})
		})))
	}
	a.server = httptest.NewTLSServer(http.HandlerFunc(a.serve))
	return a
}
func (a *operatorAPI) Close() {
	a.server.Close()
	for _, peer := range a.peers {
		peer.Close()
	}
}
func (a *operatorAPI) kube(t *testing.T) *Kubernetes {
	t.Helper()
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("token"), 0600); err != nil {
		t.Fatal(err)
	}
	return &Kubernetes{BaseURL: a.server.URL, Namespace: "default", TokenFile: token, Client: a.server.Client()}
}
func (a *operatorAPI) serve(w http.ResponseWriter, q *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch q.URL.Path {
	case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizarecoveries":
		if q.Method == "GET" {
			json.NewEncoder(w).Encode(recoveryList{Items: []Resource{*a.r}})
			return
		}
	case "/apis/rhiza.mrchypark.dev/v1alpha1/namespaces/default/rhizarecoveries/recover/status":
		if q.Method == "PUT" {
			if a.failStatus > 0 {
				a.failStatus--
				w.WriteHeader(500)
				return
			}
			var next Resource
			if json.NewDecoder(q.Body).Decode(&next) == nil {
				*a.r = next
				json.NewEncoder(w).Encode(a.r)
				return
			}
		}
	case "/apis/apps/v1/namespaces/default/statefulsets/rhiza":
		if q.Method == "GET" {
			json.NewEncoder(w).Encode(a.sts)
			return
		}
		if q.Method == "PUT" {
			var next object
			if json.NewDecoder(q.Body).Decode(&next) == nil {
				a.sts = next
				a.stsPuts++
				if number(nested(next, "spec", "replicas")) == 0 {
					a.zeroPuts++
				}
				json.NewEncoder(w).Encode(a.sts)
				return
			}
		}
	case "/api/v1/namespaces/default/secrets":
		if q.Method == "POST" {
			var next object
			if json.NewDecoder(q.Body).Decode(&next) == nil {
				if a.secret == nil {
					a.secret = next
				}
				json.NewEncoder(w).Encode(a.secret)
				return
			}
		}
	case "/api/v1/namespaces/default/pods":
		if q.Method == "GET" {
			json.NewEncoder(w).Encode(podList{Items: a.pods()})
			return
		}
	}
	if q.Method == "GET" && a.secret != nil && strings.HasPrefix(q.URL.Path, "/api/v1/namespaces/default/secrets/rhiza-recovery-") {
		json.NewEncoder(w).Encode(a.secret)
		return
	}
	http.NotFound(w, q)
}

func (a *operatorAPI) pods() []object {
	if number(nested(a.sts, "spec", "replicas")) == 0 {
		return nil
	}
	result := make([]object, 0, len(a.peers))
	for i, peer := range a.peers {
		_, port, err := net.SplitHostPort(peer.Listener.Addr().String())
		if err != nil {
			continue
		}
		p, err := strconv.Atoi(port)
		if err != nil {
			continue
		}
		result = append(result, object{
			"metadata": object{"name": string(members()[i].ID), "ownerReferences": []any{object{"uid": "sts-uid", "kind": "StatefulSet"}}},
			"status":   object{"podIP": "127.0.0.1"},
			"spec":     object{"containers": []any{object{"name": "rhiza", "ports": []any{object{"name": "http", "containerPort": float64(p)}}}}},
		})
	}
	return result
}

func fakeSTS(mode string) object {
	raw, _ := json.Marshal(members())
	return object{
		"metadata": object{"name": "rhiza", "uid": "sts-uid", "generation": float64(1), "annotations": object{}},
		"spec": object{
			"replicas": float64(3), "volumeClaimTemplates": []any{},
			"template": object{"spec": object{
				"containers": []any{object{
					"name":         "rhiza",
					"env":          []any{object{"name": "RHIZA_CLUSTER_ID", "value": "source"}, object{"name": "RHIZA_CLUSTER_MEMBERS", "value": string(raw)}, object{"name": "RHIZA_OBJSTORE_DURABILITY", "value": mode}, object{"name": "RHIZA_OBJSTORE_PREFIX", "value": "root"}, object{"name": "RHIZA_OBJSTORE_PROVIDER", "value": "s3"}, object{"name": "RHIZA_OBJSTORE_ENDPOINT", "value": "https://store.example"}, object{"name": "RHIZA_OBJSTORE_BUCKET", "value": "rhiza"}},
					"volumeMounts": []any{object{"name": "data", "mountPath": "/data"}},
				}},
				"volumes": []any{object{"name": "data", "emptyDir": object{}}},
			}},
		},
	}
}
func members() []quepaxa.Member {
	return []quepaxa.Member{{ID: "n1", URL: "http://n1", PeerURL: "quic://n1", Token: "old1"}, {ID: "n2", URL: "http://n2", PeerURL: "quic://n2", Token: "old2"}, {ID: "n3", URL: "http://n3", PeerURL: "quic://n3", Token: "old3"}}
}
func storeIdentity() map[string]string {
	return map[string]string{"RHIZA_OBJSTORE_PROVIDER": "s3", "RHIZA_OBJSTORE_ENDPOINT": "https://store.example", "RHIZA_OBJSTORE_BUCKET": "rhiza", "RHIZA_OBJSTORE_PREFIX": "root", "RHIZA_OBJSTORE_AZURE_STORAGE_ACCOUNT": ""}
}

func seedArchive(t *testing.T, ctx context.Context, bucket objstore.Bucket, prefix string, ms []quepaxa.Member, mode string) {
	t.Helper()
	wal, err := qlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: ms[0].ID, Cluster: quepaxa.Cluster{ConfigID: 1, Members: ms}, WAL: wal, Transport: testTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = core.Propose(ctx, []byte("value")); err != nil {
		t.Fatal(err)
	}
	archive := recovery.NewManager(bucket, prefix, 1)
	defer archive.Close()
	if err = archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(recovery.NewMembershipRecord("source", ms, mode))
	if err = bucket.Upload(ctx, prefix+"/voters/membership.json", bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

type testTransport struct{}

func (testTransport) SendRecord(_ context.Context, to quepaxa.NodeID, r quepaxa.RecordRequest) (quepaxa.Summary, error) {
	p := r.Proposal
	return quepaxa.Summary{RecorderID: to, Step: r.Step, FirstCurrent: &p}, nil
}
func (testTransport) SendDecision(context.Context, quepaxa.Decision) error          { return nil }
func (testTransport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) { return 0, nil }
func (testTransport) StageValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash, []byte) error {
	return nil
}
func (testTransport) FetchValue(context.Context, quepaxa.NodeID, quepaxa.ValueHash) ([]byte, error) {
	return nil, nil
}
