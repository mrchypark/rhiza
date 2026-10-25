package network

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

const (
	warmRestartHelperEnv = "RHIZA_TEST_WARM_RESTART_PEER"
	warmRestartCluster   = "warm-restart-test"
	warmRestartV0Token   = "test-peer-v0-token"
	warmRestartV1Token   = "test-peer-v1-token"
	warmRestartV2Token   = "test-peer-v2-token"
)

type warmRestartReadTipTrace struct {
	mu         sync.Mutex
	events     []string
	afterEvent func(string)
}

func (trace *warmRestartReadTipTrace) capture(event string) {
	trace.mu.Lock()
	trace.events = append(trace.events, event)
	afterEvent := trace.afterEvent
	trace.mu.Unlock()
	if afterEvent != nil {
		afterEvent(event)
	}
}

func (trace *warmRestartReadTipTrace) snapshot() []string {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return append([]string(nil), trace.events...)
}

type warmRestartPeerRequest struct {
	DataDir     string
	ListenAddr  string
	Unavailable string
}

type warmRestartPeer struct {
	cmd    *exec.Cmd
	addr   string
	waited bool
}

func TestWarmCachedQUICReadIndexAfterPeerHardRestart(t *testing.T) {
	trace := &warmRestartReadTipTrace{}
	installWarmRestartReadTipTrace(t, trace)
	dataDir := t.TempDir() + "/peer-wal"
	unavailable, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unavailableAddr := unavailable.LocalAddr().String()
	t.Cleanup(func() { _ = unavailable.Close() })
	unavailableURL := "quic://" + unavailableAddr

	peer, err := startWarmRestartPeer(t, warmRestartPeerRequest{DataDir: dataDir, ListenAddr: "127.0.0.1:0", Unavailable: unavailableURL})
	if err != nil {
		t.Fatalf("start initial peer: %v", err)
	}
	t.Cleanup(func() { peer.stopIfRunning() })

	clientMembers := warmRestartMembers(peer.addr, unavailableURL)
	clientWAL, err := qlog.Open(t.TempDir() + "/caller-wal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientWAL.Close() })
	transport := NewTransport(warmRestartCluster, "v0", &quepaxa.Cluster{ConfigID: 1, Members: clientMembers}, warmRestartV0Token)
	t.Cleanup(func() { _ = transport.Close() })
	caller, err := quepaxa.New(quepaxa.Config{
		NodeID: "v0", Cluster: quepaxa.Cluster{ConfigID: 1, Members: clientMembers}, WAL: clientWAL, Transport: transport,
	})
	if err != nil {
		t.Fatalf("create quorum caller: %v", err)
	}

	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	initialTip, err := transport.ReadTip(setupCtx, "v1")
	setupCancel()
	if err != nil || initialTip != 0 {
		t.Fatalf("warm initial authenticated peer connection: tip=%d err=%v", initialTip, err)
	}
	pool := transport.peers["v1"]
	pool.mu.Lock()
	oldConn := pool.conn
	oldHandshakeComplete := oldConn != nil && oldConn.ConnectionState().TLS.HandshakeComplete
	pool.mu.Unlock()
	if oldConn == nil || !oldHandshakeComplete {
		t.Fatalf("initial ReadTip did not leave an authenticated cached connection: cached=%t handshake=%t", oldConn != nil, oldHandshakeComplete)
	}

	if err := peer.killAndWait(); err != nil {
		t.Fatalf("SIGKILL and reap initial peer: %v", err)
	}
	restarted, err := startWarmRestartPeer(t, warmRestartPeerRequest{
		DataDir: dataDir, ListenAddr: peer.addr, Unavailable: unavailableURL,
	})
	if err != nil {
		t.Fatalf("restart peer with same endpoint and WAL: %v", err)
	}
	peer = restarted
	t.Cleanup(func() { peer.stopIfRunning() })
	if peer.addr != clientMembers[1].PeerURL[len("quic://"):] {
		t.Fatalf("restart endpoint changed: got=%s want=%s", peer.addr, clientMembers[1].PeerURL[len("quic://"):])
	}

	pool.mu.Lock()
	connBeforeReadIndex := pool.conn
	oldConnErrBefore := oldConn.Context().Err()
	pool.mu.Unlock()
	staleAtReadIndex := connBeforeReadIndex == oldConn && oldConnErrBefore == nil

	// Do not probe the restarted peer here. This single quorum operation is the
	// first post-restart request; the third voter remains unavailable.
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	index, source, readErr := caller.ReadIndex(readCtx)
	readCancel()
	pool.mu.Lock()
	connAfterReadIndex := pool.conn
	oldConnErrAfter := oldConn.Context().Err()
	newConnHandshake := connAfterReadIndex != nil && connAfterReadIndex != oldConn && connAfterReadIndex.ConnectionState().TLS.HandshakeComplete
	pool.mu.Unlock()
	t.Logf("warm_restart stale_cached_connection=%t old_context_canceled_before=%t old_context_canceled_after=%t cached_connection_replaced=%t replacement_handshake_complete=%t read_index_ok=%t read_index_source_is_voter=%t deadline=%t error_type=%T",
		staleAtReadIndex, oldConnErrBefore != nil, oldConnErrAfter != nil, connAfterReadIndex != oldConn, newConnHandshake,
		readErr == nil, source == "v0" || source == "v1", errors.Is(readErr, context.DeadlineExceeded), readErr)

	if !staleAtReadIndex {
		t.Fatalf("stale warm-cache condition was not present at the one post-restart ReadIndex: same_cached_connection=%t old_context_canceled=%t", connBeforeReadIndex == oldConn, oldConnErrBefore != nil)
	}
	if readErr != nil {
		t.Fatalf("single ReadIndex after same-endpoint hard restart failed: index=%d source=%q error=%v", index, source, readErr)
	}
	if index != 0 || (source != "v0" && source != "v1") {
		t.Fatalf("unexpected quorum result after restart: index=%d source=%q", index, source)
	}
	if warmRestartReadTipTraceEnabled() {
		want := []string{
			"network:read-tip:phase=attempt:number=1:used-cached=true:result=stateless-reset",
			"network:read-tip:phase=retry:reason=stateless-reset",
			"network:read-tip:phase=attempt:number=2:used-cached=false:result=response",
		}
		if got := trace.snapshot(); !reflect.DeepEqual(got, want) {
			t.Fatalf("ReadTip reset recovery trace = %q, want exactly %q", got, want)
		}
	}
	transport.invalidate("v1", oldConn)
	pool.mu.Lock()
	lateOldFailurePreservedNewConnection := pool.conn == connAfterReadIndex
	pool.mu.Unlock()
	if !lateOldFailurePreservedNewConnection {
		t.Fatal("late invalidation of the old connection cleared the replacement connection")
	}
}

func TestGenericMutationCallDoesNotRetryAfterStatelessReset(t *testing.T) {
	dataDir := t.TempDir() + "/peer-wal"
	unavailable, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unavailableURL := "quic://" + unavailable.LocalAddr().String()
	t.Cleanup(func() { _ = unavailable.Close() })

	peer, err := startWarmRestartPeer(t, warmRestartPeerRequest{DataDir: dataDir, ListenAddr: "127.0.0.1:0", Unavailable: unavailableURL})
	if err != nil {
		t.Fatalf("start initial peer: %v", err)
	}
	t.Cleanup(func() { peer.stopIfRunning() })
	members := warmRestartMembers(peer.addr, unavailableURL)
	transport := NewTransport(warmRestartCluster, "v0", &quepaxa.Cluster{ConfigID: 1, Members: members}, warmRestartV0Token)
	t.Cleanup(func() { _ = transport.Close() })
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = transport.ReadTip(setupCtx, "v1")
	setupCancel()
	if err != nil {
		t.Fatalf("warm initial authenticated peer connection: %v", err)
	}
	pool := transport.peers["v1"]
	pool.mu.Lock()
	oldConn := pool.conn
	pool.mu.Unlock()
	if oldConn == nil {
		t.Fatal("initial read left no cached connection")
	}
	if err := peer.killAndWait(); err != nil {
		t.Fatalf("SIGKILL and reap initial peer: %v", err)
	}
	restarted, err := startWarmRestartPeer(t, warmRestartPeerRequest{DataDir: dataDir, ListenAddr: peer.addr, Unavailable: unavailableURL})
	if err != nil {
		t.Fatalf("restart peer with same endpoint and WAL: %v", err)
	}
	peer = restarted
	t.Cleanup(func() { peer.stopIfRunning() })

	request := transport.request(peerfb.OperationLearned)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, callErr := transport.callQuorum(ctx, "v1", request)
	cancel()
	var resetErr *quic.StatelessResetError
	if !errors.As(callErr, &resetErr) {
		t.Fatalf("generic mutation call error = %T %v, want the original stateless reset", callErr, callErr)
	}
	pool.mu.Lock()
	connectionAfterError := pool.conn
	pool.mu.Unlock()
	if connectionAfterError != nil {
		t.Fatal("generic call redialed after the reset; only ReadTip may perform that retry")
	}
}

func TestReadTipResetRetryDoesNotDialAfterTransportClose(t *testing.T) {
	dataDir := t.TempDir() + "/peer-wal"
	unavailable, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unavailableURL := "quic://" + unavailable.LocalAddr().String()
	t.Cleanup(func() { _ = unavailable.Close() })
	peer, err := startWarmRestartPeer(t, warmRestartPeerRequest{DataDir: dataDir, ListenAddr: "127.0.0.1:0", Unavailable: unavailableURL})
	if err != nil {
		t.Fatalf("start initial peer: %v", err)
	}
	t.Cleanup(func() { peer.stopIfRunning() })
	members := warmRestartMembers(peer.addr, unavailableURL)
	transport := NewTransport(warmRestartCluster, "v0", &quepaxa.Cluster{ConfigID: 1, Members: members}, warmRestartV0Token)
	t.Cleanup(func() { _ = transport.Close() })
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = transport.ReadTip(setupCtx, "v1")
	setupCancel()
	if err != nil {
		t.Fatalf("warm initial authenticated peer connection: %v", err)
	}
	pool := transport.peers["v1"]
	pool.mu.Lock()
	oldConn := pool.conn
	pool.mu.Unlock()
	if oldConn == nil {
		t.Fatal("initial read left no cached connection")
	}
	if err := peer.killAndWait(); err != nil {
		t.Fatalf("SIGKILL and reap initial peer: %v", err)
	}
	peer, err = startWarmRestartPeer(t, warmRestartPeerRequest{DataDir: dataDir, ListenAddr: peer.addr, Unavailable: unavailableURL})
	if err != nil {
		t.Fatalf("restart peer with same endpoint and WAL: %v", err)
	}
	t.Cleanup(func() { peer.stopIfRunning() })

	trace := &warmRestartReadTipTrace{}
	closeOnce := sync.Once{}
	trace.afterEvent = func(event string) {
		if event == "network:read-tip:phase=attempt:number=1:used-cached=true:result=stateless-reset" {
			closeOnce.Do(func() { _ = transport.Close() })
		}
	}
	if warmRestartReadTipTraceEnabled() {
		installWarmRestartReadTipTrace(t, trace)
	} else {
		if err := transport.Close(); err != nil {
			t.Fatalf("close peer transport before read: %v", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, readErr := transport.ReadTip(ctx, "v1")
	if !errors.Is(readErr, errTransportClosed) {
		t.Fatalf("ReadTip after close at cached reset = %T %v, want transport-closed error", readErr, readErr)
	}
	if ctx.Err() != nil {
		t.Fatalf("caller context expired while testing close path: %v", ctx.Err())
	}
	cancel()
	if warmRestartReadTipTraceEnabled() {
		want := []string{
			"network:read-tip:phase=attempt:number=1:used-cached=true:result=stateless-reset",
			"network:read-tip:phase=retry:reason=stateless-reset",
			"network:read-tip:phase=attempt:number=2:used-cached=false:result=error",
		}
		if got := trace.snapshot(); !reflect.DeepEqual(got, want) {
			t.Fatalf("closed-transport ReadTip trace = %q, want exactly %q", got, want)
		}
	}
	pool.mu.Lock()
	connectionAfterClose := pool.conn
	pool.mu.Unlock()
	if connectionAfterClose != nil {
		t.Fatal("ReadTip established a replacement connection after transport close")
	}
}

func TestPeerStatelessResetKeyIsStableAndScoped(t *testing.T) {
	if got := peerStatelessResetKey(warmRestartCluster, "v1", ""); got != nil {
		t.Fatalf("empty peer token produced a reset key: %x", got)
	}
	base := peerStatelessResetKey(warmRestartCluster, "v1", warmRestartV1Token)
	if base == nil {
		t.Fatal("nonempty peer token produced no reset key")
	}
	if got := peerStatelessResetKey(warmRestartCluster, "v1", warmRestartV1Token); got == nil || *got != *base {
		t.Fatal("same cluster, node, and token did not produce a stable reset key")
	}
	for _, tc := range []struct {
		name      string
		clusterID string
		nodeID    string
		token     string
	}{
		{name: "cluster", clusterID: "other-cluster", nodeID: "v1", token: warmRestartV1Token},
		{name: "node", clusterID: warmRestartCluster, nodeID: "v2", token: warmRestartV1Token},
		{name: "token", clusterID: warmRestartCluster, nodeID: "v1", token: "other-token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := peerStatelessResetKey(types.ClusterID(tc.clusterID), quepaxa.NodeID(tc.nodeID), tc.token)
			if got == nil || *got == *base {
				t.Fatal("changed reset-key scope reused the original key")
			}
		})
	}
	certificatePublicKey := PeerPublicKey(warmRestartCluster, "v1", warmRestartV1Token)
	if strings.EqualFold(fmt.Sprintf("%x", base[:]), fmt.Sprintf("%x", certificatePublicKey)) {
		t.Fatal("reset key reused certificate public-key derivation")
	}
}

func TestReadTipResetRetryRequiresCachedTypedResetAndLiveContext(t *testing.T) {
	resetErr := &quic.StatelessResetError{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name      string
		ctx       context.Context
		cached    bool
		err       error
		wantRetry bool
	}{
		{name: "cached typed reset", ctx: context.Background(), cached: true, err: resetErr, wantRetry: true},
		{name: "uncached typed reset", ctx: context.Background(), err: resetErr},
		{name: "cached generic error", ctx: context.Background(), cached: true, err: errors.New("transport failure")},
		{name: "canceled cached reset", ctx: ctx, cached: true, err: resetErr},
		{name: "cached config rejection", ctx: context.Background(), cached: true, err: errPeerRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRetryReadTipAfterReset(tc.ctx, tc.cached, tc.err); got != tc.wantRetry {
				t.Fatalf("retry decision = %t, want %t", got, tc.wantRetry)
			}
		})
	}
}

func TestColdTransportReadIndexAfterPeerRestartControl(t *testing.T) {
	dataDir := t.TempDir() + "/peer-wal"
	unavailable, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unavailable.Close() })
	unavailableURL := "quic://" + unavailable.LocalAddr().String()

	peer, err := startWarmRestartPeer(t, warmRestartPeerRequest{DataDir: dataDir, ListenAddr: "127.0.0.1:0", Unavailable: unavailableURL})
	if err != nil {
		t.Fatalf("start initial peer: %v", err)
	}
	t.Cleanup(func() { peer.stopIfRunning() })
	if err := peer.killAndWait(); err != nil {
		t.Fatalf("SIGKILL and reap initial peer: %v", err)
	}
	peer, err = startWarmRestartPeer(t, warmRestartPeerRequest{DataDir: dataDir, ListenAddr: peer.addr, Unavailable: unavailableURL})
	if err != nil {
		t.Fatalf("restart peer with same endpoint and WAL: %v", err)
	}
	t.Cleanup(func() { peer.stopIfRunning() })

	clientMembers := warmRestartMembers(peer.addr, unavailableURL)
	clientWAL, err := qlog.Open(t.TempDir() + "/caller-wal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clientWAL.Close() })
	transport := NewTransport(warmRestartCluster, "v0", &quepaxa.Cluster{ConfigID: 1, Members: clientMembers}, warmRestartV0Token)
	t.Cleanup(func() { _ = transport.Close() })
	caller, err := quepaxa.New(quepaxa.Config{
		NodeID: "v0", Cluster: quepaxa.Cluster{ConfigID: 1, Members: clientMembers}, WAL: clientWAL, Transport: transport,
	})
	if err != nil {
		t.Fatalf("create cold-control quorum caller: %v", err)
	}
	pool := transport.peers["v1"]
	pool.mu.Lock()
	initiallyUncached := pool.conn == nil
	pool.mu.Unlock()
	if !initiallyUncached {
		t.Fatal("cold-control transport unexpectedly had a cached peer connection")
	}

	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	index, source, readErr := caller.ReadIndex(readCtx)
	readCancel()
	pool.mu.Lock()
	handshakeComplete := pool.conn != nil && pool.conn.ConnectionState().TLS.HandshakeComplete
	pool.mu.Unlock()
	t.Logf("cold_control initially_uncached=%t handshake_complete=%t read_index_ok=%t source_is_voter=%t error_type=%T",
		initiallyUncached, handshakeComplete, readErr == nil, source == "v0" || source == "v1", readErr)
	if readErr != nil || index != 0 || (source != "v0" && source != "v1") || !handshakeComplete {
		t.Fatalf("cold-transport control did not read from the restarted authenticated peer: index=%d source=%q err=%v handshake=%t", index, source, readErr, handshakeComplete)
	}
}

// TestWarmRestartPeerHelper runs only in the subprocess created above. The
// stdout line is a bind-ready signal; no health/read-tip request is used.
func TestWarmRestartPeerHelper(t *testing.T) {
	raw := os.Getenv(warmRestartHelperEnv)
	if raw == "" {
		return
	}
	var request warmRestartPeerRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	if request.DataDir == "" || request.Unavailable == "" {
		t.Fatal("incomplete warm-restart child request")
	}
	wal, err := qlog.Open(request.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	members := warmRestartMembers("", request.Unavailable)
	transport := NewTransport(warmRestartCluster, "v1", &quepaxa.Cluster{ConfigID: 1, Members: members}, warmRestartV1Token)
	core, err := quepaxa.New(quepaxa.Config{
		NodeID: "v1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: wal, Transport: transport,
	})
	if err != nil {
		_ = transport.Close()
		_ = wal.Close()
		t.Fatal(err)
	}
	server := NewServer(core, nil, warmRestartCluster, true, nil)
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	peer, err := StartPeerServer(serveCtx, request.ListenAddr, server, members, warmRestartV1Token, "")
	if err != nil {
		server.Close()
		_ = transport.Close()
		_ = wal.Close()
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(os.Stdout, "READY %s\n", peer.Addr()); err != nil {
		t.Fatal(err)
	}
	select {}
}

func startWarmRestartPeer(t *testing.T, request warmRestartPeerRequest) (*warmRestartPeer, error) {
	t.Helper()
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestWarmRestartPeerHelper$")
	cmd.Env = withoutEnv(os.Environ(), warmRestartHelperEnv)
	cmd.Env = append(cmd.Env, warmRestartHelperEnv+"="+string(payload))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	child := &warmRestartPeer{cmd: cmd}
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		return nil, err
	}
	ready := make(chan struct {
		addr string
		err  error
	}, 1)
	go func() {
		line, scanErr := bufio.NewReader(stdout).ReadString('\n')
		if scanErr != nil {
			ready <- struct {
				addr string
				err  error
			}{err: fmt.Errorf("read child bind-ready line: %w", scanErr)}
			return
		}
		fields := strings.Fields(line)
		if len(fields) != 2 || fields[0] != "READY" {
			ready <- struct {
				addr string
				err  error
			}{err: fmt.Errorf("invalid child bind-ready marker")}
			return
		}
		ready <- struct {
			addr string
			err  error
		}{addr: fields[1]}
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case result := <-ready:
		if result.err != nil {
			_ = child.killAndWait()
			return nil, fmt.Errorf("%w (child stderr=%q)", result.err, stderr.String())
		}
		child.addr = result.addr
		return child, nil
	case <-timer.C:
		_ = child.killAndWait()
		return nil, fmt.Errorf("child did not bind within 5s (stderr=%q)", stderr.String())
	}
}

func (p *warmRestartPeer) killAndWait() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil || p.waited {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return err
	}
	err := p.cmd.Wait()
	p.waited = true
	if err == nil {
		return fmt.Errorf("child exited without a killed-process status")
	}
	return nil
}

func (p *warmRestartPeer) stopIfRunning() {
	if p == nil || p.waited {
		return
	}
	_ = p.killAndWait()
}

func warmRestartMembers(v1Endpoint, unavailableEndpoint string) []quepaxa.Member {
	members := []quepaxa.Member{
		testMember(warmRestartCluster, "v0", warmRestartV0Token),
		testMember(warmRestartCluster, "v1", warmRestartV1Token),
		testMember(warmRestartCluster, "v2", warmRestartV2Token),
	}
	if v1Endpoint != "" {
		members[1].PeerURL = "quic://" + v1Endpoint
	}
	if unavailableEndpoint != "" {
		members[2].PeerURL = unavailableEndpoint
	}
	return members
}

func withoutEnv(env []string, key string) []string {
	filtered := make([]string, 0, len(env))
	prefix := key + "="
	for _, entry := range env {
		if !strings.HasPrefix(entry, prefix) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}
