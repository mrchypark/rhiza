package managedsql

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/internal/peertest"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
	"github.com/quic-go/quic-go"
)

// This bounded wire fixture exercises the actual S3 SDK, not provider parity.
// Provider contract qualification remains in the emulator/CI suites.
type sqlWireControl struct {
	hideArchive             atomic.Bool
	hideBirth               atomic.Bool
	hideApplicationStart    atomic.Bool
	injectForeignBeforeList atomic.Bool
	finalPrefix             string
	foreignStored           atomic.Bool
	dropReceiptResponse     atomic.Bool
	receiptPutReady         chan struct{}
	receiptPutRelease       chan struct{}
	putReady                chan string
	putRelease              chan struct{}
	putSuffix               string
	putPrefix               string
	putReadGateBytes        int
	putReadErrors           chan error
	putReadFailure          func(*http.Request, error)
	ownedClose              atomic.Int64
	// Evaluated before body reads/object-map lock. The owner must fail its
	// test on predicate observation errors rather than silently passing them.
	putGate func(context.Context, string) bool
	// Optional arm is installed before runtime startup. Nil retains the
	// existing unconditional gate; tests may atomically arm a later failure.
	putEnabled     *atomic.Bool
	failFirstPut   atomic.Bool
	deleteRequests atomic.Int64
	keysMu         sync.Mutex
	putKeys        []string
}

// Install before Open; only an actual owned Host Close starts this lifecycle.
type sqlWireCloseLifecycle struct {
	closing     atomic.Bool
	acceptedPUT chan string
}

func (l *sqlWireCloseLifecycle) expectedCanceledPUT(prefix string, r *http.Request, err error) bool {
	return l.closing.Load() && r.Method == http.MethodPut && errors.Is(r.Context().Err(), context.Canceled) &&
		errors.Is(err, io.ErrUnexpectedEOF) && strings.HasPrefix(r.URL.Path, prefix) && len(r.URL.Path) > len(prefix)
}

func (l *sqlWireCloseLifecycle) observePUTFailures(t *testing.T, fixture *sqlStatusFixture) {
	t.Helper()
	fixture.closeLifecycle = l
	control := fixture.source
	prefix := "/bucket/" + control.finalPrefix + "/"
	control.putReadFailure = func(r *http.Request, err error) {
		if !l.expectedCanceledPUT(prefix, r, err) {
			t.Errorf("unexpected PUT read failure method=%s path=%s canceled=%t owned_close=%t: %v", r.Method, r.URL.Path, errors.Is(r.Context().Err(), context.Canceled), l.closing.Load(), err)
		} else {
			t.Logf("expected canceled PUT method=%s path=%s canceled=true owned_close=true", r.Method, r.URL.Path)
			select {
			case l.acceptedPUT <- r.URL.Path:
			default:
			}
		}
	}
}

func expectedOwnedClosePUTReadFailure(control *sqlWireControl, r *http.Request, err error) bool {
	prefix := "/bucket/" + control.finalPrefix + "/"
	return control.ownedClose.Load() > 0 && r.Method == http.MethodPut && errors.Is(r.Context().Err(), context.Canceled) && errors.Is(err, io.ErrUnexpectedEOF) && strings.HasPrefix(r.URL.Path, prefix) && len(r.URL.Path) > len(prefix)
}

func TestOwnedClosePUTReadFailureClassification(t *testing.T) {
	control := &sqlWireControl{finalPrefix: "owned"}
	request := func(method, path string, canceled bool) *http.Request {
		r := httptest.NewRequest(method, "http://fixture"+path, strings.NewReader("partial"))
		if canceled {
			ctx, cancel := context.WithCancel(r.Context())
			cancel()
			r = r.WithContext(ctx)
		}
		return r
	}
	path := "/bucket/owned/checkpoint/CURRENT"
	control.ownedClose.Store(1)
	if !expectedOwnedClosePUTReadFailure(control, request(http.MethodPut, path, true), io.ErrUnexpectedEOF) {
		t.Fatal("owned close cancellation was not recognized")
	}
	for _, test := range []struct {
		name    string
		request *http.Request
		err     error
	}{
		{"outside owned namespace", request(http.MethodPut, "/bucket/other/checkpoint/CURRENT", true), io.ErrUnexpectedEOF},
		{"non-canceled truncation", request(http.MethodPut, path, false), io.ErrUnexpectedEOF},
		{"other read error", request(http.MethodPut, path, true), io.EOF},
		{"wrong method", request(http.MethodGet, path, true), io.ErrUnexpectedEOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			if expectedOwnedClosePUTReadFailure(control, test.request, test.err) {
				t.Fatal("unexpected PUT read failure was masked")
			}
		})
	}
	control.ownedClose.Store(0)
	if expectedOwnedClosePUTReadFailure(control, request(http.MethodPut, path, true), io.ErrUnexpectedEOF) {
		t.Fatal("canceled truncation without an owned close was masked")
	}
}

func sqlWireStore(t *testing.T) (*httptest.Server, *sqlWireControl) {
	t.Helper()
	var mu sync.Mutex
	objects := map[string][]byte{}
	control := &sqlWireControl{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			control.deleteRequests.Add(1)
		}
		var gatedBodyPrefix []byte
		gatePut := r.Method == http.MethodPut && control.putReady != nil && (control.putEnabled == nil || control.putEnabled.Load()) && strings.Contains(r.URL.Path, control.putPrefix) && strings.HasSuffix(r.URL.Path, control.putSuffix) && (control.putGate == nil || control.putGate(r.Context(), r.URL.Path))
		if gatePut && control.putReadGateBytes > 0 {
			gatedBodyPrefix = make([]byte, control.putReadGateBytes)
			n, err := io.ReadFull(r.Body, gatedBodyPrefix)
			gatedBodyPrefix = gatedBodyPrefix[:n]
			if err != nil {
				if control.putReadErrors != nil {
					select {
					case control.putReadErrors <- fmt.Errorf("%s: read gate: %w", r.URL.Path, err):
						w.WriteHeader(http.StatusBadRequest)
						return
					default:
					}
				}
				t.Errorf("read PUT gate body path=%s canceled=%t: %v", r.URL.Path, errors.Is(r.Context().Err(), context.Canceled), err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		if gatePut {
			select {
			case control.putReady <- r.URL.Path:
			case <-control.putRelease:
			case <-r.Context().Done():
				return
			}
			select {
			case <-control.putRelease:
			case <-r.Context().Done():
				if control.putReadGateBytes == 0 {
					return
				}
			}
		}
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
		if control.hideBirth.Load() && strings.HasSuffix(key, "voters/managed-sql-initialization.json") && (r.Method == http.MethodHead || r.Method == http.MethodGet) {
			w.WriteHeader(404)
			_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
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
			if control.injectForeignBeforeList.Swap(false) {
				objects[control.finalPrefix+"/foreign-history.bin"] = []byte("unexpected existing object")
				control.foreignStored.Store(true)
			}
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
			control.keysMu.Lock()
			control.putKeys = append(control.putKeys, key)
			control.keysMu.Unlock()
			if control.failFirstPut.Swap(false) {
				w.WriteHeader(500)
				_, _ = io.WriteString(w, "<Error><Code>InternalError</Code></Error>")
				return
			}
			if r.Header.Get("If-None-Match") == "*" && exists || r.Header.Get("If-Match") != "" && (!exists || r.Header.Get("If-Match") != etag) {
				w.WriteHeader(http.StatusPreconditionFailed)
				_, _ = io.WriteString(w, "<Error><Code>PreconditionFailed</Code></Error>")
				return
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				if control.putReadFailure != nil {
					control.putReadFailure(r, err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if expectedOwnedClosePUTReadFailure(control, r, err) {
					// The body was never copied into objects; only a complete
					// ReadAll may commit it.
					w.WriteHeader(http.StatusRequestTimeout)
					return
				}
				if control.putReadErrors != nil {
					select {
					case control.putReadErrors <- fmt.Errorf("%s: %w", r.URL.Path, err):
						w.WriteHeader(http.StatusBadRequest)
						return
					default:
					}
				}
				t.Errorf("read PUT body path=%s canceled=%t: %v", r.URL.Path, errors.Is(r.Context().Err(), context.Canceled), err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if len(gatedBodyPrefix) != 0 {
				body = append(gatedBodyPrefix, body...)
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
	mu                  sync.Mutex
	obj                 map[string]json.RawMessage
	failFinalize        bool
	dropFinalize        bool
	source              *sqlWireControl
	peers               map[types.NodeID]*sqlFixturePeer
	cluster             types.ClusterID
	closeLifecycle      *sqlWireCloseLifecycle
	closeObserverAtOpen atomic.Bool
}

type sqlFixturePeer struct {
	conn      net.PacketConn
	transport *quic.Transport
	once      sync.Once
	err       error
	closed    atomic.Bool
}

func (p *sqlFixturePeer) close() error {
	p.once.Do(func() { p.closed.Store(true); p.err = errors.Join(p.transport.Close(), p.conn.Close()) })
	return p.err
}

func (f *sqlStatusFixture) observeRecoveryPeer(t *testing.T, phase string, h *Host, cfg Config) {
	t.Helper()
	f.mu.Lock()
	p := f.peers[cfg.Runtime.NodeID]
	f.mu.Unlock()
	if p == nil {
		t.Fatal("recovery peer has no owned UDP reservation")
	}
	bound := p.conn.LocalAddr().String()
	endpoint := ""
	keyMatches := false
	for _, member := range cfg.Runtime.Members {
		if member.ID == cfg.Runtime.NodeID {
			endpoint = member.PeerURL
			keyMatches = member.PublicKey == quepaxa.PublicKey(network.PeerPublicKey(cfg.Runtime.ClusterID, cfg.Runtime.NodeID, cfg.Runtime.PeerToken))
		}
	}
	var socketErr error
	if udp, ok := p.conn.(*net.UDPConn); ok {
		raw, err := udp.SyscallConn()
		socketErr = err
		if err == nil {
			socketErr = raw.Control(func(uintptr) {})
		}
	} else {
		socketErr = errors.New("owned packet connection is not UDP")
	}
	h.mu.Lock()
	closed := h.closed
	var lifetimeErr error
	if h.admissionLifetime == nil {
		lifetimeErr = errors.New("Host lifetime unavailable")
	} else {
		lifetimeErr = h.admissionLifetime.Err()
	}
	h.mu.Unlock()
	state, nodeErr := h.node.ManagedState()
	t.Logf("recovery peer phase=%s node=%s endpoint=%s bound_udp=%s endpoint_matches=%t configured_addr_matches=%t socket_open=%t socket_error=%v transport_owns_socket=%t fixture_close_called=%t host_closed=%t host_lifetime_error=%v node_binding_error=%v node_R/C/A=%d/%d/%d key_matches=%t listener_state=unknown node_context=not_directly_observable", phase, cfg.Runtime.NodeID, endpoint, bound, endpoint == "quic://"+bound, cfg.Runtime.PeerAddr == bound, socketErr == nil, socketErr, p.transport.Conn == p.conn, p.closed.Load(), closed, lifetimeErr, nodeErr, state.RecorderTip, state.CertifiedTip, state.AppliedTip, keyMatches)
	if endpoint != "quic://"+bound || cfg.Runtime.PeerAddr != bound || !keyMatches {
		t.Fatal("recovery peer endpoint or pinned identity differs from its fixture reservation")
	}
}

func (f *sqlStatusFixture) reservePeer(t *testing.T, id types.NodeID) string {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &sqlFixturePeer{conn: conn, transport: &quic.Transport{Conn: conn}}
	f.mu.Lock()
	if f.peers == nil {
		f.peers = make(map[types.NodeID]*sqlFixturePeer)
	}
	if f.peers[id] != nil {
		f.mu.Unlock()
		_ = p.close()
		t.Fatal("duplicate fixture peer")
	}
	f.peers[id] = p
	f.mu.Unlock()
	t.Cleanup(func() {
		if err := p.close(); err != nil {
			t.Error(err)
		}
	})
	return conn.LocalAddr().String()
}

func (f *sqlStatusFixture) open(ctx context.Context, cfg Config) (*Host, error) {
	f.mu.Lock()
	p := f.peers[cfg.Runtime.NodeID]
	f.mu.Unlock()
	if p == nil {
		return nil, fmt.Errorf("fixture peer is not reserved")
	}
	ctx = peertest.WithTransport(ctx, string(f.cluster), string(cfg.Runtime.NodeID), p.conn.LocalAddr().String(), p.transport)
	f.closeObserverAtOpen.Store(f.closeLifecycle != nil && f.source.putReadFailure != nil)
	host, err := Open(ctx, cfg)
	if err != nil && host != nil {
		err = errors.Join(err, f.closeHost(host))
	}
	return host, err
}

func (f *sqlStatusFixture) closeHost(host *Host) error {
	f.source.ownedClose.Add(1)
	defer f.source.ownedClose.Add(-1)
	if f.closeLifecycle != nil {
		f.closeLifecycle.closing.Store(true)
	}
	return host.Close()
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
	return sqlClusterForProtocol(t, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
}

func sqlClusterForProtocol(t *testing.T, protocol string) ([]Config, *sqlStatusFixture) {
	return sqlClusterForProtocolVoters(t, protocol, 3)
}

func sqlClusterForProtocolVoters(t *testing.T, protocol string, voterCount int, reconfiguration ...bool) ([]Config, *sqlStatusFixture) {
	t.Helper()
	if len(reconfiguration) > 1 {
		t.Fatal("one reconfiguration selection required")
	}
	enableReconfiguration := len(reconfiguration) == 1 && reconfiguration[0]
	format := EvidenceFormat
	if protocol != "" {
		format = protocol
	}
	if err := operator.ValidateV1Alpha2ApplicationProfile(protocol, format); err != nil {
		t.Fatal(err)
	}
	s3, sourceControl := sqlWireStore(t)
	configs := make([]Config, voterCount)
	members := make([]quepaxa.Member, voterCount)
	clusterID, err := types.ManagedClusterID("uid", 0)
	if err != nil {
		t.Fatal(err)
	}
	namespace, err := node.ManagedNamespaceFor("", "uid", 0, "assignment", string(clusterID))
	if err != nil {
		t.Fatal(err)
	}
	sourceControl.finalPrefix = namespace.FinalPrefix
	f := &sqlStatusFixture{source: sourceControl, cluster: clusterID}
	for i := range members {
		id, err := types.ManagedNodeID(fmt.Sprintf("pod-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		addr := f.reservePeer(t, id)
		token := fmt.Sprintf("peer-%d", i)
		members[i] = quepaxa.Member{ID: id, PeerURL: "quic://" + addr, PublicKey: quepaxa.PublicKey(network.PeerPublicKey(clusterID, id, token))}
		configs[i].Runtime = RuntimeConfig{NodeID: id, ClusterID: clusterID, ObjStorePrefix: namespace.ConfigPrefix, DataDir: t.TempDir(), PeerAddr: addr, PeerToken: token, AdminToken: "admin", ObjStoreProvider: "s3", ObjStoreEndpoint: strings.TrimPrefix(s3.URL, "http://"), ObjStoreInsecure: true, ObjStoreBucket: "bucket", ObjStoreRegion: "us-east-1", ObjStoreAccessKey: "test", ObjStoreSecretKey: "test", ObjStoreDurability: types.ObjectStoreDurabilityBeforeAck, EnableReconfiguration: enableReconfiguration}
	}
	for i := range configs {
		configs[i].Runtime.Members = members
	}
	_ = json.Unmarshal([]byte(`{"apiVersion":"rhiza.mrchypark.dev/v1alpha2","kind":"RhizaCluster","metadata":{"name":"cluster","namespace":"ns","uid":"uid","resourceVersion":"1"},"spec":{"logicalID":"logical","genesisIntent":"Create","voterCount":3,"workload":{"podTemplate":{"spec":{"containers":[{"name":"app","image":"image"}]}}}},"status":{}}`), &f.obj)
	var spec map[string]json.RawMessage
	if err := json.Unmarshal(f.obj["spec"], &spec); err != nil {
		t.Fatal(err)
	}
	spec["voterCount"], _ = json.Marshal(voterCount)
	if protocol != "" {
		spec["applicationProtocol"], _ = json.Marshal(protocol)
	}
	f.obj["spec"], _ = json.Marshal(spec)
	kube := httptest.NewTLSServer(http.HandlerFunc(f.serve))
	t.Cleanup(kube.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test"), 0600); err != nil {
		t.Fatal(err)
	}
	store := &operator.V1Alpha2StatusStore{Kube: &operator.Kubernetes{BaseURL: kube.URL, Namespace: "ns", TokenFile: tokenFile, Client: kube.Client()}}
	ref := operator.V1Alpha2ClusterRef{Name: "cluster", Namespace: "ns", UID: "uid"}
	n := node.New(&configs[0].Runtime)
	source, _, err := n.PreparedStartupBindingDigestsForProtocol(protocol)
	if err != nil {
		t.Fatal(err)
	}
	common, err := n.ManagedBootstrapPlanDigestForProtocol(source, format, protocol)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := operator.NewV1Alpha2ApplicationInitPlan(ref, "logical", "deployment", "assignment", source, common, format)
	if err != nil {
		t.Fatal(err)
	}
	// The checked public plan field and CR marker agree before the first
	// authority write; no legacy assignment is converted after creation.
	plan.ApplicationProtocol = protocol
	name, _ := operator.V1Alpha2GenesisDeploymentName(ref.UID)
	bootstrap := operator.V1Alpha2BootstrapPlan{ClusterID: clusterID, PhysicalBinding: source, EvidenceFormat: format, Durability: types.ObjectStoreDurabilityBeforeAck, Reconfiguration: enableReconfiguration}
	for i, member := range members {
		bootstrap.Members = append(bootstrap.Members, operator.V1Alpha2BootstrapMember{PodUID: fmt.Sprintf("pod-%d", i), Member: member})
	}
	if _, err := operator.InitializeV1Alpha2AuthorityWithBootstrap(context.Background(), store, ref, name, plan, bootstrap); err != nil {
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

func TestManagedSQLPublicStartupRejectsRetiredPrototypeBeforeRuntimeEffects(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured string
		retired    string
	}{
		{"empty", sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, ""},
		{"mixed", sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, types.ManagedApplicationProtocolSQLKV},
		{"request", sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedRequestApplicationProtocol},
		{"graph", sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestApplicationProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			configs, fixture := sqlClusterForProtocol(t, tc.configured)
			cfg := configs[0]
			installedPlan := cfg.Plan
			cfg.Plan.ApplicationProtocol = tc.retired
			cfg.Plan.EvidenceFormat = tc.retired

			// Existing local bytes model a previously installed prototype WAL and
			// materializer. Public startup must reject the profile before opening,
			// rewriting, or rebinding either file.
			walPath := filepath.Join(cfg.Runtime.DataDir, "qlog")
			dbPath := filepath.Join(cfg.Runtime.DataDir, "sqlite.db")
			walBefore := []byte("retired-prototype-wal-bytes")
			dbBefore := []byte("retired-prototype-materializer-bytes")
			if err := os.WriteFile(walPath, walBefore, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dbPath, dbBefore, 0o600); err != nil {
				t.Fatal(err)
			}

			host, err := fixture.open(context.Background(), cfg)
			if err == nil || host != nil || !strings.Contains(err.Error(), "unsupported managed startup profile") {
				if host != nil {
					_ = fixture.closeHost(host)
				}
				t.Fatalf("retired profile was not explicitly rejected before startup: host=%v err=%v", host, err)
			}
			for path, want := range map[string][]byte{walPath: walBefore, dbPath: dbBefore} {
				got, readErr := os.ReadFile(path)
				if readErr != nil || !bytes.Equal(got, want) {
					t.Fatalf("retired startup changed existing bytes at %s: got=%q err=%v", path, got, readErr)
				}
			}
			entries, readErr := os.ReadDir(cfg.Runtime.DataDir)
			if readErr != nil || len(entries) != 2 {
				t.Fatalf("retired startup created runtime files: entries=%v err=%v", entries, readErr)
			}
			fixture.source.keysMu.Lock()
			putCount := len(fixture.source.putKeys)
			fixture.source.keysMu.Unlock()
			if putCount != 0 {
				t.Fatalf("retired startup uploaded %d objects before rejection", putCount)
			}
			state, loadErr := operator.LoadV1Alpha2ApplicationInit(context.Background(), cfg.Store, cfg.Ref)
			if loadErr != nil || state.Assignment().State != "Unused" || state.Assignment().Plan != installedPlan {
				t.Fatalf("retired startup changed initialization authority: state=%+v err=%v", state.Assignment(), loadErr)
			}
		})
	}
}

func TestManagedSQLPublicStartupPreservesSupportedProfileEvidenceError(t *testing.T) {
	for _, protocol := range []string{sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol, sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol} {
		name := protocol
		if name == "" {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			configs, fixture := sqlClusterForProtocol(t, protocol)
			cfg := configs[0]
			cfg.Plan.EvidenceFormat = "wrong-supported-profile-evidence"

			host, err := fixture.open(context.Background(), cfg)
			if host != nil {
				_ = fixture.closeHost(host)
				t.Fatal("invalid evidence unexpectedly opened a Host")
			}
			const want = "unsupported managed startup profile"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("supported profile evidence mismatch changed error contract: got %v, want %q", err, want)
			}
		})
	}
}

func TestManagedSQLFixtureRetainsPeerSocketUntilHostOpen(t *testing.T) {
	configs, f := sqlCluster(t)
	// A competing owner must not be able to acquire the advertised endpoint
	// between fixture membership selection and the real Host listener.
	competitor, bindErr := net.ListenPacket("udp", configs[0].Runtime.PeerAddr)
	if bindErr == nil {
		t.Cleanup(func() { _ = competitor.Close() })
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	host, err := f.open(ctx, configs[0])
	if host != nil {
		t.Cleanup(func() { _ = f.closeHost(host) })
	}
	if err != nil {
		t.Fatalf("fixture endpoint must remain owned through actual Host Open (competitor_bind=%v): %v", bindErr, err)
	}
	if bindErr == nil {
		t.Fatal("competing socket acquired the fixture's advertised endpoint")
	}
	if err := f.closeHost(host); err != nil {
		t.Fatal(err)
	}
	cancel()
	between, err := net.ListenPacket("udp", configs[0].Runtime.PeerAddr)
	if err == nil {
		_ = between.Close()
		t.Fatal("Host.Close released fixture socket")
	}
	// Restart uses a fresh parent, not the cancelled previous execution context.
	restarted, err := f.open(context.Background(), configs[0])
	if restarted != nil {
		t.Cleanup(func() { _ = f.closeHost(restarted) })
	}
	if err != nil {
		t.Fatalf("same transport restart: %v", err)
	}
	if err := f.closeHost(restarted); err != nil {
		t.Fatal(err)
	}
	if err := f.peers[configs[0].Runtime.NodeID].close(); err != nil {
		t.Fatal(err)
	}
	rebound, err := net.ListenPacket("udp", configs[0].Runtime.PeerAddr)
	if err != nil {
		t.Fatalf("owner cleanup retained socket: %v", err)
	}
	_ = rebound.Close()
}

func readySQL(t *testing.T, h *Host) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var last error
	for {
		// These write-oriented fixtures need quorum, unlike Service readiness.
		if last = h.node.AdmitManaged(ctx); last == nil {
			if last = h.Ready(ctx); last == nil {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(last)
		case <-time.After(30 * time.Millisecond):
		}
	}
}

func TestManagedSQLRealThreeNodeInitializationAndWrite(t *testing.T) {
	configs, f := sqlCluster(t)
	// This fixture starts background publishers and owns their Host shutdown.
	// Keep reporting incomplete PUTs unless the strict Close observer proves
	// that this fixture is closing and the owned request was canceled.
	(&sqlWireCloseLifecycle{}).observePUTFailures(t, f)
	hosts := make([]*Host, 3)
	for i, cfg := range configs[:2] {
		h, err := f.open(context.Background(), cfg)
		if h != nil {
			hosts[i] = h
			t.Cleanup(func() {
				if err := f.closeHost(h); err != nil {
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
			if err := h.Ready(ctx); err == nil {
				t.Fatal("never-admitted single voter became application ready")
			}
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), 75*time.Millisecond)
			if _, err := h.Query(ctx, QueryRequest{SQL: "SELECT 1", Consistency: "local"}); err == nil {
				t.Fatal("local query bypassed initial quorum admission")
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
	insert, err := hosts[0].Execute(context.Background(), ExecuteRequest{RequestID: "insert", SQL: "INSERT INTO app VALUES (?)", Args: []any{[]byte{0, 255}}})
	if err != nil || insert.Status != types.MutationCommitted || insert.Slot == 0 {
		t.Fatalf("insert lacks a committed ACK: %+v %v", insert, err)
	}
	a, err := operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, pending := a.Pending(); pending {
		t.Fatal("completed SQL still pending")
	}
	if evidenceHash(a.Evidence()) != init.Assignment().InitialEvidenceDigest {
		t.Fatal("ordinary write rewrote immutable E0 CR evidence")
	}
	late, err := f.open(context.Background(), configs[2])
	if late != nil {
		hosts[2] = late
		t.Cleanup(func() {
			if err := f.closeHost(late); err != nil {
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
		waitSQLCertifiedApplication(t, context.Background(), h, insert.Slot)
		// Public rows and applied slot share one SQLite snapshot; checkpoint
		// progress need not globally stop around a private evidence Inspector.
		rows, err := h.Query(context.Background(), QueryRequest{SQL: "SELECT hex(value) FROM app", Consistency: "linearizable"})
		if err != nil || rows.AppliedSlot < insert.Slot || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != "00FF" {
			t.Fatalf("replica certified SQL snapshot mismatch: %+v %v", rows, err)
		}
		status, err := h.RequestStatus(context.Background(), network.RequestStatusRequest{Kind: "sql", RequestID: "insert"})
		if err != nil || status.Receipt == nil || !reflect.DeepEqual(*status.Receipt, insert.MutationReceipt) {
			t.Fatalf("replica changed exact INSERT ACK receipt: %+v %v", status, err)
		}
	}
}

func openSQLQuorum(t *testing.T, closeLifecycle ...*sqlWireCloseLifecycle) ([]Config, []*Host, *sqlStatusFixture) {
	t.Helper()
	configs, f := sqlCluster(t)
	lifecycle := &sqlWireCloseLifecycle{acceptedPUT: make(chan string, 16)}
	if len(closeLifecycle) != 0 {
		if len(closeLifecycle) != 1 || closeLifecycle[0] == nil {
			t.Fatal("one explicit wire Close lifecycle is required")
		}
		lifecycle = closeLifecycle[0]
	}
	lifecycle.observePUTFailures(t, f)
	hosts := make([]*Host, 2)
	for i := range hosts {
		h, err := f.open(context.Background(), configs[i])
		if h != nil {
			hosts[i] = h
			t.Cleanup(func() {
				if err := f.closeHost(h); err != nil {
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

func TestManagedSQLDDLReversalDoesNotRearmMissingSource(t *testing.T) {
	configs, hosts, f := openSQLQuorum(t)
	h := hosts[0]
	initial, err := h.node.InspectManagedKV(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range []ExecuteRequest{{RequestID: "create-reversal", SQL: "CREATE TABLE reversible(value)"}, {RequestID: "drop-reversal", SQL: "DROP TABLE reversible"}} {
		if _, err := h.Execute(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	reversed, err := h.node.InspectManagedKV(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	// The user schema returns to empty, but append-only request originals and
	// receipts remain part of the application digest; they must not revert to E0.
	rows, err := h.Query(context.Background(), QueryRequest{SQL: "SELECT COUNT(*) FROM sqlite_schema WHERE name='reversible'", Consistency: "local"})
	if err != nil || len(rows.Rows) != 1 || len(rows.Rows[0]) != 1 || rows.Rows[0][0] != int64(0) {
		t.Fatalf("DDL reversal did not restore the empty user schema: %+v %v", rows, err)
	}
	if reversed.Application.Digest == initial.Application.Digest || reversed.Application.FreshEmpty || reversed.Runtime.AppliedTip <= initial.Runtime.AppliedTip {
		t.Fatal("DDL reversal lost append-only request history or rearmed E0")
	}
	for _, id := range []string{"create-reversal", "drop-reversal"} {
		status, err := h.RequestStatus(context.Background(), network.RequestStatusRequest{Kind: "sql", RequestID: id})
		if err != nil || status.Receipt == nil || status.Receipt.Status != types.MutationCommitted {
			t.Fatalf("DDL reversal receipt %s was not retained: %+v %v", id, status, err)
		}
	}
	authorityBefore, err := operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.closeHost(h); err != nil {
		t.Fatal(err)
	}
	f.source.hideArchive.Store(true)
	defer f.source.hideArchive.Store(false)
	f.source.hideApplicationStart.Store(true)
	defer f.source.hideApplicationStart.Store(false)
	reopened, err := f.open(context.Background(), configs[0])
	if reopened != nil {
		t.Cleanup(func() {
			f.source.hideArchive.Store(false)
			f.source.hideApplicationStart.Store(false)
			if err := f.closeHost(reopened); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("intact local WAL must retain non-serving recovery ownership: %v", err)
	}
	// Fresh is only the pre-open remote-absence hint. The actual retained WAL
	// and application history must independently refuse fresh-state/E0 proof.
	retained, err := reopened.node.InspectManagedKV(context.Background(), false)
	if err != nil || retained.Application != reversed.Application || retained.Runtime.WALIdentity != reversed.Runtime.WALIdentity || retained.Runtime.AppliedTip < reversed.Runtime.AppliedTip {
		t.Fatalf("reopen lost retained append-only history: %+v %v", retained, err)
	}
	if _, err := reopened.node.InspectManagedKV(context.Background(), true); err == nil {
		t.Fatal("retained application history authorized fresh-state/E0 proof")
	}
	readySQL(t, reopened) // The retained WAL and actual remaining quorum may admit.
	for _, id := range []string{"create-reversal", "drop-reversal"} {
		status, err := reopened.RequestStatus(context.Background(), network.RequestStatusRequest{Kind: "sql", RequestID: id})
		if err != nil || status.Receipt == nil || status.Receipt.Status != types.MutationCommitted || status.Receipt.Slot > reversed.Runtime.AppliedTip {
			t.Fatalf("reopened DDL receipt %s was not retained: %+v %v", id, status, err)
		}
	}
	state, err := reopened.node.ManagedState()
	if err != nil || !state.ExistingRegisteredWAL || state.CertifiedTip == 0 {
		t.Fatalf("recovery did not use actual pre-existing WAL: %+v %v", state, err)
	}
	// The fault hides reads, not the actual CAS object. Restore visibility
	// before normal shutdown flushes the shared archive; otherwise GET-404 plus
	// an existing HEAD creates an artificial permanent If-None-Match conflict.
	f.source.hideArchive.Store(false)
	f.source.hideApplicationStart.Store(false)
	if err := f.closeHost(reopened); err != nil {
		t.Fatal(err)
	}
	if err := f.closeHost(hosts[1]); err != nil {
		t.Fatal(err)
	}
	readWAL := func() []qlog.Entry {
		t.Helper()
		wal, err := qlog.OpenReadOnly(filepath.Join(configs[0].Runtime.DataDir, "qlog"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := wal.Close(); err != nil {
				t.Error(err)
			}
		}()
		nonce, registered := wal.Identity()
		if !registered || fmt.Sprintf("%x", nonce) != reversed.Runtime.WALIdentity {
			t.Fatal("missing-source restart replaced the registered WAL identity")
		}
		entries, err := wal.Read()
		if err != nil {
			t.Fatal(err)
		}
		return entries
	}
	beforeWAL := readWAL()
	// Without quorum, Open may itself fail while recovering the certified log,
	// or return a non-serving owner. Neither result may reinitialize local data.
	f.source.hideArchive.Store(true)
	f.source.hideApplicationStart.Store(true)
	openCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	blocked, openErr := f.open(openCtx, configs[0])
	cancel()
	if blocked != nil {
		t.Cleanup(func() {
			f.source.hideArchive.Store(false)
			f.source.hideApplicationStart.Store(false)
			if err := f.closeHost(blocked); err != nil {
				t.Error(err)
			}
		})
	}
	if openErr != nil && !errors.Is(openErr, context.DeadlineExceeded) && !errors.Is(openErr, context.Canceled) && !errors.Is(openErr, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("unexpected no-quorum startup failure: %v", openErr)
	}
	if blocked != nil {
		if openErr == nil {
			readyCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			err := blocked.Ready(readyCtx)
			cancel()
			if err == nil {
				t.Error("remote absence bypassed initial quorum admission")
			}
		}
		f.source.hideArchive.Store(false)
		f.source.hideApplicationStart.Store(false)
		if err := f.closeHost(blocked); err != nil {
			t.Fatal(err)
		}
	} else if openErr == nil {
		t.Fatal("no-quorum startup returned neither owner nor error")
	}
	f.source.hideArchive.Store(false)
	f.source.hideApplicationStart.Store(false)
	afterWAL := readWAL()
	if len(afterWAL) < len(beforeWAL) || !reflect.DeepEqual(afterWAL[:len(beforeWAL)], beforeWAL) {
		t.Fatal("no-quorum startup lost or replaced retained WAL history")
	}
	application, _, err := materializer.InspectManagedAppendOnlyRequestFile(context.Background(), filepath.Join(configs[0].Runtime.DataDir, "sqlite.db"), types.DefaultIdempotencyWindowSlots)
	if err != nil || application != reversed.Application {
		t.Fatalf("no-quorum startup changed retained application/receipt history: %+v %v", application, err)
	}
	live, err := operator.LoadV1Alpha2Admission(context.Background(), configs[0].Store, configs[0].Ref)
	if err != nil || live.EverApplicationReserved() || live.ResourceVersion() != authorityBefore.ResourceVersion() || !bytes.Equal(live.Evidence(), authorityBefore.Evidence()) {
		t.Fatal("append-only DDL reversal changed CR initialization or reservation history", err)
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
				if err := f.closeHost(h); err != nil {
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
			h, err := f.open(context.Background(), configs[2])
			if h != nil {
				t.Cleanup(func() {
					if err := f.closeHost(h); err != nil {
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
			if !missingHead && !strings.Contains(err.Error(), "application source history") && !strings.Contains(err.Error(), "application reservation history") {
				t.Fatalf("neither certified source nor canonical reservation history blocked initialization: %v", err)
			}
		})
	}
}

func sqlSourceNode(t *testing.T, cfg Config) (*node.Node, node.ManagedStartupIdentity) {
	t.Helper()
	n := node.New(&cfg.Runtime)
	source, member, err := n.PreparedStartupBindingDigestsForProtocol(cfg.Plan.ApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	common, err := n.ManagedBootstrapPlanDigestForProtocol(source, cfg.Plan.EvidenceFormat, cfg.Plan.ApplicationProtocol)
	if err != nil {
		t.Fatal(err)
	}
	return n, node.ManagedStartupIdentity{Member: recovery.PreparedStartupIdentity{ClusterUID: cfg.Ref.UID, Generation: cfg.Plan.Generation, DeploymentUID: cfg.Plan.DeploymentUID, PodUID: cfg.PodUID, NodeID: string(cfg.Runtime.NodeID), SourceBinding: source, ConfigDigest: member}, AssignmentID: cfg.Plan.AssignmentID, IntentHash: cfg.Plan.IntentHash, StorageID: cfg.Plan.StorageID, PhysicalBinding: source, BootstrapPlanDigest: common, EvidenceFormat: cfg.Plan.EvidenceFormat, ApplicationProtocol: cfg.Plan.ApplicationProtocol}
}

func TestManagedSQLSourceDefaultS3Provider(t *testing.T) {
	configs, f := sqlCluster(t)
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
	h, err := f.open(context.Background(), cfg)
	if h != nil {
		t.Cleanup(func() {
			if err := f.closeHost(h); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatalf("public host did not normalize default S3 provider: %v", err)
	}
}

func TestManagedSQLSourceDiagnosticRejectsUnexpectedExistingObject(t *testing.T) {
	configs, f := sqlCluster(t)
	f.source.injectForeignBeforeList.Store(true)
	n, i := sqlSourceNode(t, configs[0])
	empty, err := n.PrepareManagedSQLSource(context.Background(), i, true)
	if !f.source.foreignStored.Load() {
		t.Fatal("fixture did not provide unexpected existing object")
	}
	if err == nil && empty {
		t.Fatal("diagnostic LIST failed to reject unexpected existing object")
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

func TestManagedSQLConsumedMissingBirthIsNeverRecreated(t *testing.T) {
	configs, hosts, f := openSQLQuorum(t)
	if err := f.closeHost(hosts[0]); err != nil {
		t.Fatal(err)
	}
	// A missing consumed receipt is an error, even before the first SQL/HEAD.
	countBirth := func() int {
		f.source.keysMu.Lock()
		defer f.source.keysMu.Unlock()
		count := 0
		for _, key := range f.source.putKeys {
			if strings.HasSuffix(key, "voters/managed-sql-initialization.json") {
				count++
			}
		}
		return count
	}
	before := countBirth()
	f.source.hideBirth.Store(true)
	defer f.source.hideBirth.Store(false)
	h, err := f.open(context.Background(), configs[0])
	if h != nil {
		t.Cleanup(func() { _ = f.closeHost(h) })
	}
	if err == nil {
		t.Fatal("Consumed missing birth was recreated")
	}
	if countBirth() != before {
		t.Fatal("Consumed missing birth triggered a new receipt PUT")
	}
}

func TestManagedSQLSDKOldDelayedPutAndRetryStayOldNamespace(t *testing.T) {
	source, control := sqlWireStore(t)
	control.putReady = make(chan string, 4)
	control.putRelease = make(chan struct{})
	control.failFirstPut.Store(true)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(control.putRelease) }) })
	oldCluster, _ := types.ManagedClusterID("uid", 0)
	newCluster, _ := types.ManagedClusterID("uid", 1)
	oldNS, _ := node.ManagedNamespaceFor("installed", "uid", 0, "old-assignment", string(oldCluster))
	newNS, _ := node.ManagedNamespaceFor("installed", "uid", 1, "new-assignment", string(newCluster))
	cfg := objectstore.Config{Provider: objectstore.ProviderS3, Endpoint: strings.TrimPrefix(source.URL, "http://"), Bucket: "bucket", Region: "us-east-1", Insecure: true, AccessKey: "test", SecretKey: "test", MaxRetries: 3, Prefix: oldNS.ConfigPrefix}
	old, err := objectstore.NewBucketForNamespace(context.Background(), cfg, oldNS.FinalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- old.Upload(ctx, oldNS.FinalPrefix+"/archive/head.bin", strings.NewReader("old request"))
	}()
	select {
	case key := <-control.putReady:
		if !strings.Contains(key, oldNS.FinalPrefix) {
			t.Fatal("wrong initial SDK dispatch")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// Constructor-input changes cannot retarget an already-selected SDK key.
	cfg.Prefix = newNS.ConfigPrefix
	successor, err := objectstore.NewBucketForNamespace(context.Background(), cfg, newNS.FinalPrefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := successor.Close(); err != nil {
			t.Error(err)
		}
	})
	cfg.Endpoint = "127.0.0.1:1"
	cfg.Bucket = "successor-bucket"
	release.Do(func() { close(control.putRelease) })
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := old.Upload(ctx, newNS.FinalPrefix+"/archive/head.bin", strings.NewReader("cross-generation")); err == nil {
		t.Fatal("old SDK handle accepted successor prefix")
	}
	control.keysMu.Lock()
	keys := append([]string(nil), control.putKeys...)
	control.keysMu.Unlock()
	if len(keys) < 2 {
		t.Fatal("fixture did not exercise an actual SDK retry")
	}
	for _, key := range keys {
		if !strings.HasPrefix(key, oldNS.FinalPrefix+"/") {
			t.Fatalf("old SDK retry reached successor: %s", key)
		}
	}
	if exists, err := successor.Exists(ctx, newNS.FinalPrefix+"/archive/head.bin"); err != nil || exists {
		t.Fatalf("successor affected: exists=%v error=%v", exists, err)
	}
}

func TestManagedSQLCallerMutationCannotRepointRuntime(t *testing.T) {
	configs, hosts, _ := openSQLQuorum(t)
	h := hosts[0]
	before, _, err := h.node.PreparedStartupBindingDigests()
	if err != nil {
		t.Fatal(err)
	}
	configs[0].Runtime.ObjStorePrefix = "successor"
	configs[0].Runtime.ObjStoreEndpoint = "127.0.0.1:1"
	configs[0].Runtime.Members[0].PeerURL = "quic://127.0.0.1:1"
	configs[0].Runtime.Members[0].PublicKey = quepaxa.PublicKey{}
	after, _, err := h.node.PreparedStartupBindingDigests()
	if err != nil || before != after {
		t.Fatal("caller mutated live runtime binding")
	}
	if _, err := h.Execute(context.Background(), ExecuteRequest{RequestID: "immutable-config", SQL: "CREATE TABLE app(value)"}); err != nil {
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
			if err := f.closeHost(h); err != nil {
				t.Error(err)
			}
		}
	})
	for _, cfg := range configs {
		go func() { h, err := f.open(ctx, cfg); results <- result{h, err} }()
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
