package managedsql

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

// This bounded wire fixture exercises the actual S3 SDK, not provider parity.
// Provider contract qualification remains in the emulator/CI suites.
type sqlWireControl struct {
	hideArchive                atomic.Bool
	hideApplicationStart       atomic.Bool
	injectForeignBeforeReceipt atomic.Bool
	foreignStored              atomic.Bool
	dropReceiptResponse        atomic.Bool
	receiptPutReady            chan struct{}
	receiptPutRelease          chan struct{}
}

func sqlWireStore(t *testing.T) (*httptest.Server, *sqlWireControl) {
	t.Helper()
	var mu sync.Mutex
	objects := map[string][]byte{}
	control := &sqlWireControl{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "voters/managed-sql-initialization.json") && control.receiptPutReady != nil {
			select {
			case control.receiptPutReady <- struct{}{}:
			case <-control.receiptPutRelease:
			case <-r.Context().Done():
				return
			}
			select {
			case <-control.receiptPutRelease:
			case <-r.Context().Done():
				return
			}
		}
		mu.Lock()
		defer mu.Unlock()
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		if control.hideApplicationStart.Load() && strings.HasSuffix(key, "voters/managed-sql-application-start.json") && (r.Method == http.MethodHead || r.Method == http.MethodGet) {
			w.WriteHeader(404)
			_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		if control.hideArchive.Load() && strings.HasSuffix(key, "archive/head.bin") && (r.Method == http.MethodHead || r.Method == http.MethodGet) {
			w.WriteHeader(404)
			_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		if r.Method == http.MethodGet && (r.URL.Query().Get("list-type") != "" || r.URL.Query().Has("prefix")) {
			type item struct {
				Key          string
				Size         int
				ETag         string
				LastModified string
			}
			v := struct {
				XMLName     xml.Name `xml:"ListBucketResult"`
				Name        string
				IsTruncated bool
				Contents    []item
			}{Name: "bucket"}
			for k, b := range objects {
				if control.hideArchive.Load() && strings.Contains(r.URL.Query().Get("prefix"), "archive/extents") {
					continue
				}
				if strings.HasPrefix(k, r.URL.Query().Get("prefix")) {
					v.Contents = append(v.Contents, item{k, len(b), fmt.Sprintf("\"%x\"", md5.Sum(b)), "2026-10-30T00:00:00Z"})
				}
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(v)
			return
		}
		b, exists := objects[key]
		etag := fmt.Sprintf("\"%x\"", md5.Sum(b))
		if r.Method == http.MethodPut {
			if strings.HasSuffix(key, "voters/managed-sql-initialization.json") && control.injectForeignBeforeReceipt.Swap(false) {
				objects["consensus/foreign-history.bin"] = []byte("different-key writer after empty LIST")
				control.foreignStored.Store(true)
			}
			if r.Header.Get("If-None-Match") == "*" && exists || r.Header.Get("If-Match") != "" && (!exists || r.Header.Get("If-Match") != etag) {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = io.WriteString(w, "<Error><Code>PreconditionFailed</Code></Error>")
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			if strings.HasPrefix(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") {
				var decoded []byte
				for {
					end := bytes.Index(body, []byte("\r\n"))
					if end < 0 {
						t.Error("invalid signed chunk")
						w.WriteHeader(400)
						return
					}
					n, e := strconv.ParseInt(strings.SplitN(string(body[:end]), ";", 2)[0], 16, 64)
					body = body[end+2:]
					if e != nil || n < 0 || n > int64(len(body)) {
						t.Error("invalid signed chunk size")
						w.WriteHeader(400)
						return
					}
					if n == 0 {
						break
					}
					decoded = append(decoded, body[:int(n)]...)
					body = body[int(n)+2:]
				}
				body = decoded
			}
			objects[key] = body
			if strings.HasSuffix(key, "voters/managed-sql-initialization.json") && control.dropReceiptResponse.Swap(false) {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err == nil {
					_ = conn.Close()
				}
				return
			}
			w.Header().Set("ETag", fmt.Sprintf("\"%x\"", md5.Sum(body)))
			return
		}
		if !exists {
			w.WriteHeader(404)
			_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Length", strconv.Itoa(len(b)))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		if r.Method == http.MethodGet {
			_, _ = w.Write(b)
		}
	}))
	t.Cleanup(s.Close)
	return s, control
}

type sqlStatusFixture struct {
	mu           sync.Mutex
	obj          map[string]json.RawMessage
	failFinalize bool
	dropFinalize bool
	source       *sqlWireControl
}

func (f *sqlStatusFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if strings.Contains(r.URL.Path, "/deployments/") {
		name, _ := operator.V1Alpha2GenesisDeploymentName("uid")
		_, _ = fmt.Fprintf(w, `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":%q,"namespace":"ns","uid":"deployment","resourceVersion":"1","labels":{"rhiza.mrchypark.dev/cluster-uid":"uid","rhiza.mrchypark.dev/generation":"0"},"ownerReferences":[{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","name":"cluster","uid":"uid","controller":true}]}}`, name)
		return
	}
	if r.Method == http.MethodPut {
		var next map[string]json.RawMessage
		if json.NewDecoder(r.Body).Decode(&next) != nil {
			w.WriteHeader(400)
			return
		}
		var currentMeta, newMeta map[string]any
		_ = json.Unmarshal(f.obj["metadata"], &currentMeta)
		_ = json.Unmarshal(next["metadata"], &newMeta)
		if currentMeta["resourceVersion"] != newMeta["resourceVersion"] {
			w.WriteHeader(409)
			return
		}
		var oldState, newState struct {
			Authority struct {
				PendingWrite bool `json:"pendingWrite"`
			} `json:"authority"`
		}
		_ = json.Unmarshal(f.obj["status"], &oldState)
		_ = json.Unmarshal(next["status"], &newState)
		finalizing := oldState.Authority.PendingWrite && !newState.Authority.PendingWrite
		if f.failFinalize {
			if finalizing {
				w.WriteHeader(500)
				_, _ = io.WriteString(w, `{"error":"injected finalization failure"}`)
				return
			}
		}
		rv, _ := strconv.Atoi(currentMeta["resourceVersion"].(string))
		newMeta["resourceVersion"] = strconv.Itoa(rv + 1)
		next["metadata"], _ = json.Marshal(newMeta)
		f.obj = next
		if finalizing && f.dropFinalize {
			f.dropFinalize = false
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
	}
	_ = json.NewEncoder(w).Encode(f.obj)
}

func sqlCluster(t *testing.T) ([]Config, *sqlStatusFixture) {
	t.Helper()
	s3, sourceControl := sqlWireStore(t)
	configs := make([]Config, 3)
	members := make([]quepaxa.Member, 3)
	for i := range members {
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := conn.LocalAddr().String()
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		id := quepaxa.NodeID(fmt.Sprintf("n%d", i))
		token := fmt.Sprintf("peer-%d", i)
		members[i] = quepaxa.Member{ID: id, PeerURL: "quic://" + addr, PublicKey: quepaxa.PublicKey(network.PeerPublicKey("consensus", id, token))}
		configs[i].Runtime = RuntimeConfig{NodeID: id, ClusterID: "consensus", DataDir: t.TempDir(), PeerAddr: addr, PeerToken: token, AdminToken: "admin", ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(s3.URL, "http://"), ObjStoreInsecure: true, ObjStoreBucket: "bucket", ObjStoreRegion: "us-east-1", ObjStoreAccessKey: "test", ObjStoreSecretKey: "test", ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck}
	}
	for i := range configs {
		configs[i].Runtime.Members = members
	}
	f := &sqlStatusFixture{source: sourceControl}
	_ = json.Unmarshal([]byte(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"cluster","namespace":"ns","uid":"uid","resourceVersion":"1"},"spec":{"logicalID":"logical","genesisIntent":"Create","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image"}]}}}},"status":{}}`), &f.obj)
	kube := httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(kube.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	store := &operator.V1Alpha2StatusStore{Kube: &operator.Kubernetes{BaseURL: kube.URL, Namespace: "ns", TokenFile: tokenFile, Client: kube.Client()}}
	ref := operator.V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
	n := node.New(&configs[0].Runtime)
	source, _, err := n.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	common, err := n.ManagedBootstrapPlanDigest(source, EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := operator.NewV1Alpha2ApplicationInitPlan(ref, "logical", "deployment", "assignment", source, common, EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	name, _ := operator.V1Alpha2GenesisDeploymentName(ref.UID)
	if _, err := operator.InitializeV1Alpha2AuthorityWithPlan(context.Background(), store, ref, name, plan); err != nil {
		t.Fatal(err)
	}
	for i := range configs {
		configs[i].Store = store
		configs[i].Ref = ref
		configs[i].Plan = plan
		configs[i].PodUID = fmt.Sprintf("pod-%d", i)
	}
	return configs, f
}

func readySQL(t *testing.T, h *Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var last error
	for {
		if last = h.Ready(ctx); last == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(last)
		case <-time.After(30 * time.Millisecond):
		}
	}
}

func TestManagedSQLRealThreeNodeInitializationAndWrite(t *testing.T) {
	configs, _ := sqlCluster(t)
	hosts := make([]*Host, 3)
	for i, cfg := range configs[:2] {
		h, err := Open(context.Background(), cfg)
		if h != nil {
			hosts[i] = h
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatalf("open member %d: %v", i, err)
		}
		if _, err := h.node.API(); err == nil {
			t.Fatal("managed raw API exposed")
		}
		if _, err := h.node.Handler(); err == nil {
			t.Fatal("managed raw handler exposed")
		}
		if i == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
			if h.Ready(ctx) == nil {
				t.Fatal("single voter admitted without quorum")
			}
			cancel()
			s, err := operator.LoadV1Alpha2ApplicationInit(context.Background(), cfg.Store, cfg.Ref)
			if err != nil || s.Assignment().State != "Consumed" {
				t.Fatalf("E0 required quorum: %v", err)
			}
		}
	}
	for _, h := range hosts[:2] {
		readySQL(t, h)
	}
	init, err := operator.LoadV1Alpha2ApplicationInit(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil || init.Assignment().State != "Consumed" {
		t.Fatalf("initial assignment: %v", err)
	}
	response, err := hosts[0].Execute(context.Background(), ExecuteRequest{RequestID: "create", SQL: "CREATE TABLE app(value BLOB)"})
	if err != nil {
		t.Fatalf("create: %+v %v", response, err)
	}
	_, err = hosts[0].Execute(context.Background(), ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO app VALUES (?)", Args: []any{[]byte{0, 255}}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := a.Pending(); pending {
		t.Fatal("completed SQL still pending")
	}
	if evidenceHash(a.Evidence()) == init.Assignment().InitialEvidenceDigest {
		t.Fatal("write did not advance application evidence")
	}
	late, err := Open(context.Background(), configs[2])
	if late != nil {
		hosts[2] = late
		t.Cleanup(func() {
			if err := late.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("late advanced-evidence member: %v", err)
	}
	for _, h := range hosts {
		readySQL(t, h)
		if _, err := h.node.API(); err == nil {
			t.Fatal("admitted raw API exposed")
		}
		if _, err := h.node.Handler(); err == nil {
			t.Fatal("admitted raw handler exposed")
		}
		state, err := h.node.InspectManagedSQL(context.Background(), false)
		if err != nil || !bytes.Equal(encodeEvidence(state), a.Evidence()) {
			t.Fatalf("replica SQL evidence mismatch: %v", err)
		}
	}
}

func openSQLQuorum(t *testing.T) ([]Config, []*Host, *sqlStatusFixture) {
	t.Helper()
	configs, f := sqlCluster(t)
	hosts := make([]*Host, 2)
	for i := range hosts {
		h, err := Open(context.Background(), configs[i])
		if h != nil {
			hosts[i] = h
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, h := range hosts {
		readySQL(t, h)
	}
	return configs, hosts, f
}

func TestManagedSQLPendingDurableOriginalResolvesWithoutNewExecution(t *testing.T) {
	configs, hosts, f := openSQLQuorum(t)
	h := hosts[0]
	if _, err := h.Execute(context.Background(), ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE app(value BLOB)"}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.failFinalize = true
	f.mu.Unlock()
	req := ExecuteRequest{RequestID: "durable-blob", SQL: "INSERT INTO app VALUES (?)", Args: []any{[]byte{0, 255}}}
	if _, err := h.Execute(context.Background(), req); err == nil {
		t.Fatal("unconfirmed finalization reported success")
	}
	a, err := operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	original, ok, err := a.PendingSQL()
	if err != nil || !ok || !bytes.Equal(original.Args[0].([]byte), []byte{0, 255}) {
		t.Fatalf("durable original blob missing: %v", err)
	}
	before, err := h.node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	if h.Ready(context.Background()) == nil {
		t.Fatal("unknown finalization reopened admission")
	}
	if _, err := h.Execute(context.Background(), ExecuteRequest{RequestID: "blocked", SQL: "INSERT INTO app VALUES (1)"}); err == nil {
		t.Fatal("new write bypassed pending uncertainty")
	}
	f.mu.Lock()
	f.failFinalize = false
	f.mu.Unlock()
	if err := h.RecoverPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, err := h.node.ManagedState()
	if err != nil || after.CertifiedTip != before.CertifiedTip {
		t.Fatalf("resolution re-executed SQL: before=%+v after=%+v error=%v", before, after, err)
	}
	readySQL(t, h)
	a, err = operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.Pending(); ok {
		t.Fatal("resolved request remained pending")
	}
}

func TestManagedSQLUnknownPendingIsNotNoEffectAndBlocksNewSQL(t *testing.T) {
	configs, hosts, _ := openSQLQuorum(t)
	h := hosts[0]
	a, err := operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	req := ExecuteRequest{RequestID: "reserved-not-executed", SQL: "CREATE TABLE must_not_exist(value)"}
	if _, _, err := operator.ReserveV1Alpha2SQLWrite(context.Background(), configs[0].Store, a, a.Authority(), a.Evidence(), req); err != nil {
		t.Fatal(err)
	}
	before, err := h.node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.RecoverPending(context.Background()); err == nil {
		t.Fatal("unknown request became no-effect proof")
	}
	if h.Ready(context.Background()) == nil {
		t.Fatal("unknown request admitted new SQL")
	}
	a, err = operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.Pending(); !ok {
		t.Fatal("unknown request was cleared")
	}
	after, err := h.node.ManagedState()
	if err != nil || after.CertifiedTip != before.CertifiedTip {
		t.Fatal("unknown pending request was re-executed")
	}
}

func TestManagedSQLLostFinalizeResponseRequiresFreshRecoveryAdmission(t *testing.T) {
	configs, hosts, f := openSQLQuorum(t)
	h := hosts[0]
	f.mu.Lock()
	f.dropFinalize = true
	f.mu.Unlock()
	if _, err := h.Execute(context.Background(), ExecuteRequest{RequestID: "lost-response", SQL: "CREATE TABLE app(value)"}); err == nil {
		t.Fatal("lost response established writer ownership")
	}
	if h.Ready(context.Background()) == nil {
		t.Fatal("unknown finalization automatically reopened")
	}
	a, err := operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := a.Pending(); ok {
		t.Fatal("injected finalize did not apply")
	}
	before, err := h.node.ManagedState()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.RecoverPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	readySQL(t, h)
	after, err := h.node.ManagedState()
	if err != nil || before.CertifiedTip != after.CertifiedTip {
		t.Fatal("readback recovery re-executed SQL")
	}
}

func TestManagedSQLDDLReversalDoesNotRearmMissingSource(t *testing.T) {
	configs, hosts, f := openSQLQuorum(t)
	h := hosts[0]
	initial, err := h.node.InspectManagedSQL(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range []ExecuteRequest{{RequestID: "create-reversal", SQL: "CREATE TABLE reversible(value)"}, {RequestID: "drop-reversal", SQL: "DROP TABLE reversible"}} {
		if _, err := h.Execute(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	reversed, err := h.node.InspectManagedSQL(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if reversed.Application.Digest != initial.Application.Digest || reversed.Application.FreshEmpty {
		t.Fatal("regression requires E0 logical digest but nonfresh actual history")
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	f.source.hideArchive.Store(true)
	defer f.source.hideArchive.Store(false)
	reopened, err := Open(context.Background(), configs[0])
	if reopened != nil {
		t.Cleanup(func() {
			if err := reopened.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	if err == nil {
		t.Fatal("consumed E0 logical digest rearmed missing-source bootstrap after real DDL")
	}
}

func TestManagedSQLUnusedAssignmentRejectsPreexistingCertifiedSource(t *testing.T) {
	for _, missingHead := range []bool{true, false} {
		t.Run(fmt.Sprintf("missing_HEAD_%t", missingHead), func(t *testing.T) {
			configs, hosts, f := openSQLQuorum(t)
			if _, err := hosts[0].Execute(context.Background(), ExecuteRequest{RequestID: "historical", SQL: "CREATE TABLE historical(value)"}); err != nil {
				t.Fatal(err)
			}
			for _, h := range hosts {
				if err := h.Close(); err != nil {
					t.Fatal(err)
				}
			}
			// Corrupt/import an unused assignment while genuine certified history is
			// retained remotely. The empty third member must not initialize from absence.
			f.mu.Lock()
			var status, authority, assignment map[string]json.RawMessage
			_ = json.Unmarshal(f.obj["status"], &status)
			_ = json.Unmarshal(status["authority"], &authority)
			_ = json.Unmarshal(status["applicationInit"], &assignment)
			delete(authority, "evidence")
			assignment["state"] = json.RawMessage(`"Unused"`)
			delete(assignment, "initialEvidenceDigest")
			status["authority"], _ = json.Marshal(authority)
			status["applicationInit"], _ = json.Marshal(assignment)
			f.obj["status"], _ = json.Marshal(status)
			f.mu.Unlock()
			f.source.hideArchive.Store(missingHead)
			f.source.hideApplicationStart.Store(!missingHead)
			h, err := Open(context.Background(), configs[2])
			if h != nil {
				t.Cleanup(func() {
					if err := h.Close(); err != nil {
						t.Error(err)
					}
				})
			}
			if err == nil {
				t.Fatal("unused assignment and empty local WAL consumed E0 over existing certified SQL source")
			}
			init, loadErr := operator.LoadV1Alpha2ApplicationInit(context.Background(), configs[2].Store, configs[2].Ref)
			if loadErr != nil || init.Assignment().State != "Unused" {
				t.Fatalf("existing-source rejection changed assignment: %v", loadErr)
			}
			if !missingHead && !strings.Contains(err.Error(), "application source history") {
				t.Fatalf("actual certified source was not inspected: %v", err)
			}
		})
	}
}

func sqlSourceNode(t *testing.T, cfg Config) (*node.Node, node.ManagedStartupIdentity) {
	t.Helper()
	n := node.New(&cfg.Runtime)
	source, member, err := n.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	common, err := n.ManagedBootstrapPlanDigest(source, EvidenceFormat)
	if err != nil {
		t.Fatal(err)
	}
	return n, node.ManagedStartupIdentity{Member: recovery.PreparedStartupIdentity{ClusterUID: cfg.Ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, PodUID: cfg.PodUID, NodeID: string(cfg.Runtime.NodeID), SourceBinding: source, ConfigDigest: member}, AssignmentID: cfg.Plan.AssignmentID, IntentHash: cfg.Plan.IntentHash, StorageID: cfg.Plan.StorageID, PhysicalBinding: source, BootstrapPlanDigest: common, EvidenceFormat: EvidenceFormat}
}

func TestManagedSQLSourceDefaultS3Provider(t *testing.T) {
	configs, _ := sqlCluster(t)
	cfg := configs[0]
	cfg.Runtime.ObjStoreProvider = ""
	n, i := sqlSourceNode(t, cfg)
	if i.PhysicalBinding != cfg.Plan.PhysicalBinding {
		t.Fatal("omitted provider changed default S3 binding")
	}
	empty, err := n.PrepareManagedSQLSource(context.Background(), i, true)
	if err != nil || !empty {
		t.Fatalf("omitted provider must use configured S3 SDK: empty=%v error=%v", empty, err)
	}
	if err := n.VerifyManagedSQLFreshSource(context.Background(), i); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSQLSourceForeignKeyInterleaveReviewHold(t *testing.T) {
	configs, f := sqlCluster(t)
	f.source.injectForeignBeforeReceipt.Store(true)
	n, i := sqlSourceNode(t, configs[0])
	empty, err := n.PrepareManagedSQLSource(context.Background(), i, true)
	if !f.source.foreignStored.Load() {
		t.Fatal("fixture did not write distinct key between empty LIST and receipt CAS")
	}
	if err == nil && empty {
		t.Fatal("REVIEW HOLD: empty LIST plus receipt IfNotExists accepted a concurrent distinct-key write; this is not an atomic prefix claim")
	}
}

func TestManagedSQLSourceLostReceiptResponseRetry(t *testing.T) {
	configs, f := sqlCluster(t)
	f.source.dropReceiptResponse.Store(true)
	n, i := sqlSourceNode(t, configs[0])
	_, firstErr := n.PrepareManagedSQLSource(context.Background(), i, true)
	t.Logf("first receipt response observation: %v", firstErr)
	empty, err := n.PrepareManagedSQLSource(context.Background(), i, true)
	if err != nil || !empty {
		t.Fatalf("exact receipt retry: empty=%v error=%v", empty, err)
	}
	if err := n.VerifyManagedSQLFreshSource(context.Background(), i); err != nil {
		t.Fatal(err)
	}
}

func TestManagedSQLConcurrentLegitimateInitialMembers(t *testing.T) {
	configs, f := sqlCluster(t)
	f.source.receiptPutReady = make(chan struct{}, 3)
	f.source.receiptPutRelease = make(chan struct{})
	type result struct {
		h   *Host
		err error
	}
	results := make(chan result, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	var release sync.Once
	pending := 3
	var owned []*Host
	t.Cleanup(func() {
		release.Do(func() { close(f.source.receiptPutRelease) })
		cancel()
		for pending > 0 {
			select {
			case r := <-results:
				pending--
				if r.h != nil {
					owned = append(owned, r.h)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("%d initializer results remain unverified after cancellation", pending)
				pending = 0
			}
		}
		for _, h := range owned {
			if err := h.Close(); err != nil {
				t.Error(err)
			}
		}
	})
	for _, cfg := range configs {
		go func() { h, err := Open(ctx, cfg); results <- result{h, err} }()
	}
	for range 2 {
		select {
		case <-f.source.receiptPutReady:
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent initializers did not both observe empty namespace")
		}
	}
	release.Do(func() { close(f.source.receiptPutRelease) })
	for range 3 {
		select {
		case r := <-results:
			pending--
			if r.h != nil {
				owned = append(owned, r.h)
			}
			if r.err != nil {
				t.Errorf("legitimate concurrent member: %v", r.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("concurrent member open did not finish")
		}
	}
}
