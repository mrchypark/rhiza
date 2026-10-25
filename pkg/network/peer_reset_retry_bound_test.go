//go:build rhiza_local_testhooks

package network

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestReadTipResetRetryStopsAfterSecondPeerFailure(t *testing.T) {
	trace := &warmRestartReadTipTrace{}
	installWarmRestartReadTipTrace(t, trace)
	dataDir := t.TempDir() + "/peer-wal"
	unavailable, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	unavailableURL := "quic://" + unavailable.LocalAddr().String()
	t.Cleanup(func() { _ = unavailable.Close() })

	child, err := startWarmRestartPeer(t, warmRestartPeerRequest{
		DataDir: dataDir, ListenAddr: "127.0.0.1:0", Unavailable: unavailableURL,
	})
	if err != nil {
		t.Fatalf("start initial voter: %v", err)
	}
	t.Cleanup(func() { child.stopIfRunning() })
	members := warmRestartMembers(child.addr, unavailableURL)
	transport := NewTransport(warmRestartCluster, "v0", &quepaxa.Cluster{ConfigID: 1, Members: members}, warmRestartV0Token)
	t.Cleanup(func() { _ = transport.Close() })
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if _, err := transport.ReadTip(setupCtx, "v1"); err != nil {
		setupCancel()
		t.Fatalf("establish initial authenticated connection: %v", err)
	}
	setupCancel()
	pool := transport.peers["v1"]
	pool.mu.Lock()
	oldConn := pool.conn
	pool.mu.Unlock()
	if oldConn == nil || !oldConn.ConnectionState().TLS.HandshakeComplete {
		t.Fatal("initial request did not leave an authenticated cached connection")
	}
	if err := child.killAndWait(); err != nil {
		t.Fatalf("SIGKILL and reap initial voter: %v", err)
	}

	// The replacement has the same authenticated identity and reset key, but is
	// a learner. It returns a real peer-level quorum error to the retry instead
	// of serving a successful ReadIndex response.
	learnerWAL, err := qlog.Open(t.TempDir() + "/learner-wal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = learnerWAL.Close() })
	learnerCluster := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{members[0], members[2]}}
	learnerTransport := NewTransport(warmRestartCluster, "v1", &learnerCluster, warmRestartV1Token)
	t.Cleanup(func() { _ = learnerTransport.Close() })
	learnerCore, err := quepaxa.NewLearner(quepaxa.Config{
		NodeID: "v1", Cluster: learnerCluster, WAL: learnerWAL, Transport: learnerTransport,
	})
	if err != nil {
		t.Fatalf("create nonvoting replacement core: %v", err)
	}
	learnerTransport.BindCore(learnerCore)
	server := NewServer(learnerCore, nil, warmRestartCluster, true, learnerTransport)
	t.Cleanup(server.Close)
	restartedPeer, err := StartPeerServer(context.Background(), child.addr, server, members, warmRestartV1Token, "")
	if err != nil {
		t.Fatalf("bind authenticated replacement at same endpoint: %v", err)
	}
	t.Cleanup(func() { _ = restartedPeer.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, readErr := transport.ReadTip(ctx, "v1")
	if !errors.Is(readErr, quepaxa.ErrQuorumUnavailable) {
		cancel()
		t.Fatalf("second peer response error = %T %v, want quorum-unavailable", readErr, readErr)
	}
	if ctx.Err() != nil {
		cancel()
		t.Fatalf("ReadTip replaced or exhausted the caller context: %v", ctx.Err())
	}
	cancel()

	want := []string{
		"network:read-tip:phase=attempt:number=1:used-cached=true:result=stateless-reset",
		"network:read-tip:phase=retry:reason=stateless-reset",
		"network:read-tip:phase=attempt:number=2:used-cached=false:result=error",
	}
	if got := trace.snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadTip retry trace = %q, want exactly two attempts: %q", got, want)
	}

	pool.mu.Lock()
	replacement := pool.conn
	active := len(pool.active)
	pool.mu.Unlock()
	if replacement == nil || replacement == oldConn || !replacement.ConnectionState().TLS.HandshakeComplete {
		t.Fatal("second attempt did not leave exactly the authenticated replacement connection cached")
	}
	if active != 0 {
		t.Fatalf("second failed attempt leaked %d active connection references", active)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("close caller transport after second failure: %v", err)
	}
	pool.mu.Lock()
	leftCached := pool.conn
	leftActive := len(pool.active)
	pool.mu.Unlock()
	if leftCached != nil || leftActive != 0 {
		t.Fatalf("caller transport cleanup left cached=%t active=%d", leftCached != nil, leftActive)
	}
}
