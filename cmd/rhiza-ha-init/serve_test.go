package main

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

	"github.com/mrchypark/rhiza/internal/peertest"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedsql"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestCheckReadyUsesPrivateBearerAndRequiresNoContent(t *testing.T) {
	status := http.StatusNoContent
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/readyz" || r.Header.Get("Authorization") != "Bearer private-token" {
			t.Error("incorrect readiness request")
			http.Error(w, "bad", 400)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(s.Close)
	if err := checkReady(context.Background(), s.Client(), s.URL+"/readyz", "private-token"); err != nil {
		t.Fatal(err)
	}
	status = http.StatusServiceUnavailable
	if err := checkReady(context.Background(), s.Client(), s.URL+"/readyz", "private-token"); err == nil || strings.Contains(err.Error(), "private-token") {
		t.Fatal("failed readiness accepted or token leaked")
	}
	if err := checkReady(context.Background(), s.Client(), s.URL+"/readyz", ""); err == nil {
		t.Fatal("empty token accepted")
	}
}

// This bounded S3 wire fixture exercises the real Host/QUIC/HTTP path, not
// provider parity. The API fixture can cancel finalization after durable SQL.
func managedHTTPQuorum(t *testing.T) (*managedsql.Host, managedsql.Config, *httptest.Server, *atomic.Bool, <-chan struct{}) {
	t.Helper()
	ctx := context.Background()
	cluster, _ := types.ManagedClusterID("uid", 0)
	ns, err := node.ManagedNamespaceFor("", "uid", 0, "assignment", string(cluster))
	if err != nil {
		t.Fatal(err)
	}
	archive := managedHTTPWireStore(t)
	configs := make([]managedsql.Config, 3)
	members := make([]quepaxa.Member, 3)
	transports := make([]*quic.Transport, 3)
	for i := range configs {
		conn, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := conn.LocalAddr().String()
		transport := &quic.Transport{Conn: conn}
		transports[i] = transport
		t.Cleanup(func() {
			if err := transport.Close(); err != nil {
				t.Error(err)
			}
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		})
		id, _ := types.ManagedNodeID(fmt.Sprintf("pod-%d", i))
		token := fmt.Sprintf("peer-%d", i)
		members[i] = quepaxa.Member{ID: id, PeerURL: "quic://" + addr, PublicKey: quepaxa.PublicKey(network.PeerPublicKey(cluster, id, token))}
		configs[i].Runtime = managedsql.RuntimeConfig{NodeID: id, ClusterID: cluster, ObjStorePrefix: ns.ConfigPrefix, DataDir: t.TempDir(), PeerAddr: addr, PeerToken: token, AdminToken: "private-admin", ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(archive.URL, "http://"), ObjStoreInsecure: true, ObjStoreBucket: "bucket", ObjStoreRegion: "us-east-1", ObjStoreAccessKey: "test", ObjStoreSecretKey: "test", ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck}
	}
	for i := range configs {
		configs[i].Runtime.Members = members
	}
	assertEndpointsHeld := func() {
		t.Helper()
		for _, config := range configs {
			conn, err := net.ListenPacket("udp", config.Runtime.PeerAddr)
			if err == nil {
				_ = conn.Close()
				t.Fatal("CLI fixture endpoint released")
			}
		}
	}
	assertEndpointsHeld()
	var mu sync.Mutex
	var obj map[string]json.RawMessage
	if err := json.Unmarshal([]byte(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"cluster","namespace":"ns","uid":"uid","resourceVersion":"1"},"spec":{"logicalID":"logical","genesisIntent":"Create","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image"}]}}}},"status":{}}`), &obj); err != nil {
		t.Fatal(err)
	}
	blockFinalize := new(atomic.Bool)
	finalizeStarted := make(chan struct{}, 1)
	depName, _ := operator.V1Alpha2GenesisDeploymentName("uid")
	kubeServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if strings.Contains(r.URL.Path, "/deployments/") {
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": depName, "namespace": "ns", "uid": "deployment", "resourceVersion": "1", "labels": map[string]any{"rhiza.mrchypark.dev/cluster-uid": "uid", "rhiza.mrchypark.dev/generation": "0"}, "ownerReferences": []any{map[string]any{"apiVersion": operator.V1Alpha2APIVersion, "kind": "RhizaCluster", "name": "cluster", "uid": "uid", "controller": true}}}})
			return
		}
		if r.Method == http.MethodPut {
			var next map[string]json.RawMessage
			if json.NewDecoder(r.Body).Decode(&next) != nil {
				w.WriteHeader(400)
				return
			}
			var currentMeta, newMeta map[string]any
			_ = json.Unmarshal(obj["metadata"], &currentMeta)
			_ = json.Unmarshal(next["metadata"], &newMeta)
			if currentMeta["resourceVersion"] != newMeta["resourceVersion"] {
				w.WriteHeader(409)
				return
			}
			var before, after struct {
				Authority struct {
					PendingWrite bool `json:"pendingWrite"`
				} `json:"authority"`
			}
			_ = json.Unmarshal(obj["status"], &before)
			_ = json.Unmarshal(next["status"], &after)
			if before.Authority.PendingWrite && !after.Authority.PendingWrite && blockFinalize.Load() {
				finalizeStarted <- struct{}{}
				<-r.Context().Done()
				return
			}
			rv, _ := strconv.Atoi(currentMeta["resourceVersion"].(string))
			newMeta["resourceVersion"] = strconv.Itoa(rv + 1)
			next["metadata"], _ = json.Marshal(newMeta)
			obj = next
		}
		_ = json.NewEncoder(w).Encode(obj)
	}))
	t.Cleanup(kubeServer.Close)
	tokenFile := filepath.Join(t.TempDir(), "kube-token")
	if err := os.WriteFile(tokenFile, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	store := &operator.V1Alpha2StatusStore{Kube: &operator.Kubernetes{BaseURL: kubeServer.URL, Namespace: "ns", TokenFile: tokenFile, Client: kubeServer.Client()}}
	ref := operator.V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
	n := node.New(&configs[0].Runtime)
	source, _, err := n.PreparedStartupBindingDigestsForProtocol(sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := n.ManagedBootstrapPlanDigestForProtocol(source, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := operator.NewV1Alpha2ApplicationInitPlan(ref, "logical", "deployment", "assignment", source, digest, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	plan.ApplicationProtocol = sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol
	var spec map[string]json.RawMessage
	_ = json.Unmarshal(obj["spec"], &spec)
	spec["applicationProtocol"], _ = json.Marshal(plan.ApplicationProtocol)
	obj["spec"], _ = json.Marshal(spec)
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: cluster, PhysicalBinding: source, EvidenceFormat: sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, Durability: types.ObjectStoreDurabilityBeforeAck}
	for i, member := range members {
		bootstrap.Members = append(bootstrap.Members, operator.V1Alpha2BootstrapMember{PodUID: fmt.Sprintf("pod-%d", i), Member: member})
	}
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(ctx, store, ref, depName, plan, bootstrap); err != nil {
		t.Fatal(err)
	}
	var hosts []*managedsql.Host
	for i := 0; i < 2; i++ {
		configs[i].Store, configs[i].Ref, configs[i].Plan, configs[i].PodUID = store, ref, plan, fmt.Sprintf("pod-%d", i)
		peerCtx := peertest.WithTransport(ctx, string(cluster), string(configs[i].Runtime.NodeID), configs[i].Runtime.PeerAddr, transports[i])
		h, err := managedsql.Open(peerCtx, configs[i])
		if h != nil {
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, h)
	}
	readyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		if err := hosts[0].RecoverPending(readyCtx); err == nil {
			break
		} else if readyCtx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
	}
	handler, err := managedHandler(hosts[0], "private-admin")
	if err != nil {
		t.Fatal(err)
	}
	assertEndpointsHeld()
	httpServer := httptest.NewServer(handler)
	t.Cleanup(httpServer.Close)
	return hosts[0], configs[0], httpServer, blockFinalize, finalizeStarted
}

func managedHTTPWireStore(t *testing.T) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	objects := map[string][]byte{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		key := strings.TrimPrefix(r.URL.Path, "/bucket/")
		if r.Method == http.MethodGet && (r.URL.Query().Get("list-type") != "" || r.URL.Query().Has("prefix")) {
			type item struct {
				Key                string
				Size               int
				ETag, LastModified string
			}
			list := struct {
				XMLName     xml.Name `xml:"ListBucketResult"`
				Name        string
				IsTruncated bool
				Contents    []item
			}{Name: "bucket"}
			for key, data := range objects {
				if strings.HasPrefix(key, r.URL.Query().Get("prefix")) {
					list.Contents = append(list.Contents, item{key, len(data), fmt.Sprintf("\"%x\"", md5.Sum(data)), "2026-11-01T00:00:00Z"})
				}
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(list)
			return
		}
		data, exists := objects[key]
		etag := fmt.Sprintf("\"%x\"", md5.Sum(data))
		if r.Method == http.MethodPut {
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
					n, err := strconv.ParseInt(strings.SplitN(string(body[:end]), ";", 2)[0], 16, 64)
					body = body[end+2:]
					if err != nil || n < 0 || n > int64(len(body)) {
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
			w.Header().Set("ETag", fmt.Sprintf("\"%x\"", md5.Sum(body)))
			return
		}
		if !exists {
			w.WriteHeader(404)
			_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func managedHTTPPost(t *testing.T, server *httptest.Server, route string, body any) (int, []byte) {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	r, err := http.NewRequest(http.MethodPost, server.URL+route, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer private-admin")
	response, err := server.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err = io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, data
}

func TestManagedHandlerCommittedWriteAndRecoveryDoNotReserveCR(t *testing.T) {
	_, cfg, server, _, _ := managedHTTPQuorum(t)
	before, err := operator.LoadV1Alpha2Admission(context.Background(), cfg.Store, cfg.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if code, body := managedHTTPPost(t, server, "/execute", managedsql.ExecuteRequest{RequestID: "schema", SQL: "CREATE TABLE pending_http(value)"}); code != http.StatusOK {
		t.Fatalf("schema: %d %s", code, body)
	}
	if code, body := managedHTTPPost(t, server, "/execute", managedsql.ExecuteRequest{RequestID: "write", SQL: "INSERT INTO pending_http VALUES (1)"}); code != http.StatusOK {
		t.Fatalf("write: %d %s", code, body)
	}
	if code, body := managedHTTPPost(t, server, "/query", managedsql.QueryRequest{SQL: "SELECT COUNT(*) FROM pending_http", Consistency: "local"}); code != http.StatusOK || !bytes.Contains(body, []byte(`"rows":[[1]]`)) {
		t.Fatalf("exactly-once result: %d %s", code, body)
	}
	after, err := operator.LoadV1Alpha2Admission(context.Background(), cfg.Store, cfg.Ref)
	if err != nil || before.ResourceVersion() != after.ResourceVersion() || !bytes.Equal(before.Evidence(), after.Evidence()) {
		t.Fatal("ordinary HTTP write mutated CR authority", err)
	}
}

func TestManagedHandlerUnselectedRecoveryRemainsUnknown(t *testing.T) {
	_, cfg, server, _, _ := managedHTTPQuorum(t)
	before, err := operator.LoadV1Alpha2Admission(context.Background(), cfg.Store, cfg.Ref)
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := managedHTTPPost(t, server, "/recover-pending", struct{}{}); code != http.StatusServiceUnavailable {
		t.Fatalf("missing exact selector was treated as completed: %d", code)
	}
	after, err := operator.LoadV1Alpha2Admission(context.Background(), cfg.Store, cfg.Ref)
	if err != nil || before.ResourceVersion() != after.ResourceVersion() || !bytes.Equal(before.Evidence(), after.Evidence()) {
		t.Fatal("unselected UNKNOWN recovery mutated CR", err)
	}
}

func TestManagedHandlerAuthenticationAndClosedRoutes(t *testing.T) {
	if _, err := managedHandler(nil, "token"); err == nil {
		t.Fatal("nil host accepted")
	}
	if _, err := managedHandler(&managedsql.Host{}, ""); err == nil {
		t.Fatal("empty token accepted")
	}
	h, err := managedHandler(&managedsql.Host{}, "secret")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path, auth string
		status     int
	}{
		{"/readyz", "", http.StatusUnauthorized}, {"/execute", "Bearer wrong", http.StatusUnauthorized},
		{"/kv", "Bearer secret", http.StatusNotFound}, {"/graph", "Bearer secret", http.StatusNotFound},
		{"/execute", "Bearer secret", http.StatusMethodNotAllowed},
		{"/recover-pending", "", http.StatusUnauthorized},
		{"/recover-pending", "Bearer wrong", http.StatusUnauthorized},
		{"/recover-pending", "Bearer secret", http.StatusMethodNotAllowed},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		req.Header.Set("Authorization", tc.auth)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != tc.status || strings.Contains(w.Body.String(), "secret") {
			t.Fatalf("%s status %d", tc.path, w.Code)
		}
	}
	for _, body := range []string{`{"sql":"x","unknown":true}`, `{} {}`, strings.Repeat(" ", (1<<20)+1)} {
		req := httptest.NewRequest(http.MethodPost, "/execute", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("invalid body status %d", w.Code)
		}
	}
	for _, body := range []string{`{"request_id":"foreign"}`, `{"sql":"INSERT INTO app VALUES (1)"}`, `{} {}`, strings.Repeat(" ", (1<<20)+1)} {
		req := httptest.NewRequest(http.MethodPost, "/recover-pending", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer secret")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("recovery accepted replacement request: %d", w.Code)
		}
	}
}
