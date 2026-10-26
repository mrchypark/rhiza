//go:build rhiza_local_testhooks

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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

const mutationResponseLossHelperEnv = "RHIZA_TEST_MUTATION_RESPONSE_LOSS_PEER"

type mutationResponseLossPeerRequest struct {
	WALDir          string `json:"wal_dir"`
	MaterializerDB  string `json:"materializer_db"`
	EventPath       string `json:"event_path"`
	BlockAfterApply bool   `json:"block_after_apply"`
}

type mutationResponseLossPeer struct {
	cmd      *exec.Cmd
	addr     string
	lines    chan string
	readErr  chan error
	outputMu sync.Mutex
	output   strings.Builder
	stderr   strings.Builder
	waited   bool
}

type mutationResponseLossResult struct {
	err error
}

type mutationResponseLossTransport struct {
	quepaxa.Transport
	calls atomic.Int32
	mu    sync.Mutex
	err   error
}

func (t *mutationResponseLossTransport) SendDecision(ctx context.Context, decision quepaxa.Decision) error {
	t.calls.Add(1)
	err := t.Transport.SendDecision(ctx, decision)
	t.mu.Lock()
	t.err = err
	t.mu.Unlock()
	return err
}

func (t *mutationResponseLossTransport) decisionError() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

func TestLearnedMutationAppliedOnceWhenResponseIsLost(t *testing.T) {
	root := t.TempDir()
	walDir := root + "/peer-wal"
	dbPath := root + "/peer.sqlite"
	eventPath := root + "/handler-events.log"
	udpSocket, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("reserve stable peer socket: %v", err)
	}
	t.Cleanup(func() { _ = udpSocket.Close() })
	endpoint := udpSocket.LocalAddr().String()

	var children []*mutationResponseLossPeer
	stopChildren := func() {
		for i := len(children) - 1; i >= 0; i-- {
			children[i].stop()
		}
	}
	t.Cleanup(stopChildren)

	first, err := startMutationResponseLossPeer(t, mutationResponseLossPeerRequest{
		WALDir: walDir, MaterializerDB: dbPath, EventPath: eventPath,
		BlockAfterApply: true,
	}, udpSocket)
	if err != nil {
		t.Fatalf("start initial peer: %v", err)
	}
	children = append(children, first)

	members := mutationResponseLossMembers(first.addr)
	if first.addr != endpoint {
		t.Fatalf("child endpoint=%q, want parent-reserved endpoint %q", first.addr, endpoint)
	}
	transport := NewTransport(warmRestartCluster, "v0", &quepaxa.Cluster{ConfigID: 1, Members: members}, warmRestartV0Token)
	t.Cleanup(func() { _ = transport.Close() })
	counted := &mutationResponseLossTransport{Transport: transport}
	callerWAL, err := qlog.Open(root + "/caller-wal")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = callerWAL.Close() })
	caller, err := quepaxa.New(quepaxa.Config{
		NodeID: "v0", Cluster: quepaxa.Cluster{ConfigID: 1, Members: members}, WAL: callerWAL, Transport: counted,
	})
	if err != nil {
		t.Fatalf("create caller core: %v", err)
	}

	const requestID = "mutation-response-loss-1"
	const key = "response-loss-key"
	const wantValue = "applied-before-response-loss"
	item, err := types.EncodeKVBatchItem(types.KVCommand{
		RequestID: requestID, Operation: "put", Key: key, Value: []byte(wantValue), ObservedAtUnixMS: time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatal(err)
	}
	value := types.AssembleKVBatch([][]byte{item})

	proposalCtx, cancelProposal := context.WithTimeout(context.Background(), 5*time.Second)
	slot, _, err := caller.ProposeCertified(proposalCtx, value)
	cancelProposal()
	if err != nil {
		t.Fatalf("certify mutation before learned delivery: %v", err)
	}
	if slot != 1 {
		t.Fatalf("certified slot=%d, want first slot 1", slot)
	}
	pool := transport.peers["v1"]
	pool.mu.Lock()
	oldConn := pool.conn
	oldHandshake := oldConn != nil && oldConn.ConnectionState().TLS.HandshakeComplete
	pool.mu.Unlock()
	if oldConn == nil || !oldHandshake {
		t.Fatalf("record RPC did not establish authenticated cached connection: cached=%t handshake=%t", oldConn != nil, oldHandshake)
	}

	completeCtx, cancelComplete := context.WithTimeout(context.Background(), 5*time.Second)
	completeDone := make(chan mutationResponseLossResult, 1)
	go func() {
		_, completeErr := caller.CompleteDecision(completeCtx, slot)
		completeDone <- mutationResponseLossResult{err: completeErr}
	}()

	line, err := first.waitLine("APPLIED slot=1", 5*time.Second)
	if err != nil {
		cancelComplete()
		t.Fatalf("handler never reached durable apply before response: %v", err)
	}
	if line != "APPLIED slot=1" {
		cancelComplete()
		t.Fatalf("unexpected handler event %q", line)
	}
	if err := first.killAndWait(); err != nil {
		cancelComplete()
		t.Fatalf("kill and reap peer after durable apply but before response: %v", err)
	}
	restarted, err := startMutationResponseLossPeer(t, mutationResponseLossPeerRequest{
		WALDir: walDir, MaterializerDB: dbPath, EventPath: eventPath,
		BlockAfterApply: true,
	}, udpSocket)
	if err != nil {
		cancelComplete()
		t.Fatalf("restart peer on same endpoint and state while response is outstanding: %v", err)
	}
	children = append(children, restarted)
	if restarted.addr != first.addr {
		cancelComplete()
		t.Fatalf("restart endpoint=%q, want original endpoint %q", restarted.addr, first.addr)
	}
	// The original Learned stream is waiting for a response, so make one
	// separate read-only RPC on the stale connection to elicit the restarted
	// transport's stateless reset. This does not resend or complete the mutation.
	resetCtx, cancelReset := context.WithTimeout(context.Background(), 2*time.Second)
	resetRequest := transport.request(peerfb.OperationSync)
	resetRequest.From, resetRequest.Limit = 1, 1
	_, resetTriggerErr := transport.callQuorum(resetCtx, "v1", resetRequest)
	cancelReset()
	var triggerReset *quic.StatelessResetError
	if !errors.As(resetTriggerErr, &triggerReset) {
		cancelComplete()
		t.Fatalf("read-only reset trigger error=%T %v, want stateless reset on stale connection", resetTriggerErr, resetTriggerErr)
	}

	var completionErr error
	select {
	case result := <-completeDone:
		completionErr = result.err
	case <-completeCtx.Done():
		cancelComplete()
		_ = restarted.killAndWait()
		events, eventErr := os.ReadFile(eventPath)
		t.Fatalf("mutation RPC did not resolve after same-identity server restart: %v (apply events=%q read error=%v)", completeCtx.Err(), events, eventErr)
	}
	if err := restarted.killAndWait(); err != nil {
		cancelComplete()
		t.Fatalf("kill and reap same-endpoint restart helper: %v", err)
	}
	cancelComplete()
	if !errors.Is(completionErr, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("CompleteDecision error=%T %v, want quorum-unavailable after lost response", completionErr, completionErr)
	}
	var resetErr *quic.StatelessResetError
	if !errors.As(counted.decisionError(), &resetErr) {
		t.Fatalf("underlying learned RPC error=%T %v, want stateless reset", counted.decisionError(), counted.decisionError())
	}
	if got := counted.calls.Load(); got != 1 {
		t.Fatalf("logical SendDecision attempts=%d, want exactly 1", got)
	}
	pool.mu.Lock()
	cacheAfterLoss := pool.conn
	pool.mu.Unlock()
	if cacheAfterLoss != nil {
		t.Fatal("lost learned response left the reset connection cached")
	}

	events, err := os.ReadFile(eventPath)
	if err != nil {
		t.Fatalf("read private handler event count: %v", err)
	}
	if got, want := strings.Count(string(events), "APPLIED slot=1\n"), 1; got != want {
		t.Fatalf("actual handler apply events=%d, want exactly %d; events=%q", got, want, events)
	}

	material, err := materializer.Open(dbPath, 1)
	if err != nil {
		t.Fatalf("reopen peer materializer after process loss: %v", err)
	}
	t.Cleanup(func() { _ = material.Close() })
	gotValue, found, err := material.KVGet(context.Background(), key, time.Now())
	if err != nil || !found || string(gotValue) != wantValue {
		t.Fatalf("persisted KV value=%q found=%t err=%v, want %q", gotValue, found, err, wantValue)
	}
	receipt, found, err := material.MutationReceipt(context.Background(), types.MutationKV, requestID)
	if err != nil || !found || receipt.Status != types.MutationCommitted || receipt.Slot != uint64(slot) {
		t.Fatalf("persisted mutation receipt=%+v found=%t err=%v, want committed slot %d", receipt, found, err, slot)
	}
	if tip := material.Tip(); tip != uint64(slot) {
		t.Fatalf("persisted materializer tip=%d, want %d", tip, slot)
	}
	t.Logf("processed_mutation_response_lost=true slot=%d apply_events=1 send_decision_calls=%d stateless_reset=true persisted_receipt=true", slot, counted.calls.Load())
}

// TestMutationResponseLossPeerHelper is run only in the subprocess above. The
// apply-complete hook blocks the real handler before it returns to the QUIC
// stream writer, making the parent-killed response loss deterministic.
func TestMutationResponseLossPeerHelper(t *testing.T) {
	raw := os.Getenv(mutationResponseLossHelperEnv)
	if raw == "" {
		return
	}
	var request mutationResponseLossPeerRequest
	if err := json.Unmarshal([]byte(raw), &request); err != nil {
		t.Fatal(err)
	}
	if request.WALDir == "" || request.MaterializerDB == "" || request.EventPath == "" {
		t.Fatal("incomplete mutation-response-loss child request")
	}
	wal, err := qlog.Open(request.WALDir)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	material, err := materializer.Open(request.MaterializerDB, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	members := mutationResponseLossMembers("")
	cluster := quepaxa.Cluster{ConfigID: 1, Members: members}
	serverTransport := NewTransport(warmRestartCluster, "v1", &cluster, warmRestartV1Token)
	defer serverTransport.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "v1", Cluster: cluster, WAL: wal, Transport: serverTransport})
	if err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, material, warmRestartCluster, true, nil)
	if request.BlockAfterApply {
		localtesthooks.Set(func(event string) {
			if event != "network:learned:phase=apply-complete:node=v1:sender=v0:slot=1" {
				return
			}
			file, openErr := os.OpenFile(request.EventPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if openErr != nil {
				fmt.Fprintf(os.Stderr, "write handler event: %v\n", openErr)
				os.Exit(2)
			}
			_, writeErr := fmt.Fprintln(file, "APPLIED slot=1")
			syncErr := file.Sync()
			closeErr := file.Close()
			if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
				fmt.Fprintf(os.Stderr, "sync handler event: %v\n", err)
				os.Exit(2)
			}
			fmt.Fprintln(os.Stdout, "APPLIED slot=1")
			_ = os.Stdout.Sync()
			select {}
		})
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	packetFile := os.NewFile(3, "mutation-response-loss-peer-udp")
	if packetFile == nil {
		t.Fatal("missing inherited UDP socket")
	}
	packetConn, err := net.FilePacketConn(packetFile)
	_ = packetFile.Close()
	if err != nil {
		t.Fatalf("open inherited UDP socket: %v", err)
	}
	udpConn, ok := packetConn.(*net.UDPConn)
	if !ok {
		_ = packetConn.Close()
		t.Fatalf("inherited packet socket has type %T, want *net.UDPConn", packetConn)
	}
	peerTransport := &quic.Transport{
		Conn: udpConn, StatelessResetKey: peerStatelessResetKey(warmRestartCluster, "v1", warmRestartV1Token),
	}
	defer peerTransport.Close()
	peer, err := StartPeerServerOnTransport(serveCtx, peerTransport, server, members, warmRestartV1Token, "")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	if _, err := fmt.Fprintf(os.Stdout, "READY %s\n", peer.Addr()); err != nil {
		t.Fatal(err)
	}
	_ = os.Stdout.Sync()
	select {}
}

func startMutationResponseLossPeer(t *testing.T, request mutationResponseLossPeerRequest, socket *net.UDPConn) (*mutationResponseLossPeer, error) {
	t.Helper()
	if socket == nil {
		return nil, fmt.Errorf("parent-owned UDP socket is required")
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMutationResponseLossPeerHelper$")
	cmd.Env = withoutEnv(os.Environ(), mutationResponseLossHelperEnv)
	cmd.Env = append(cmd.Env, mutationResponseLossHelperEnv+"="+string(payload))
	socketFile, err := socket.File()
	if err != nil {
		return nil, fmt.Errorf("duplicate parent UDP socket: %w", err)
	}
	cmd.ExtraFiles = []*os.File{socketFile}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = socketFile.Close()
		return nil, err
	}
	child := &mutationResponseLossPeer{cmd: cmd, lines: make(chan string, 4), readErr: make(chan error, 1)}
	cmd.Stderr = &child.stderr
	if err := cmd.Start(); err != nil {
		_ = socketFile.Close()
		_ = stdout.Close()
		return nil, err
	}
	_ = socketFile.Close()
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			child.outputMu.Lock()
			_, _ = child.output.WriteString(line + "\n")
			child.outputMu.Unlock()
			select {
			case child.lines <- line:
			default:
			}
		}
		child.readErr <- scanner.Err()
		close(child.lines)
	}()
	line, err := child.waitLinePrefix("READY ", 5*time.Second)
	if err != nil {
		_ = child.killAndWait()
		return nil, fmt.Errorf("child did not bind (stdout=%q stderr=%q): %w", child.outputText(), child.stderr.String(), err)
	}
	child.addr = strings.TrimPrefix(line, "READY ")
	if child.addr == "" {
		_ = child.killAndWait()
		return nil, fmt.Errorf("child emitted empty bind address")
	}
	return child, nil
}

func (p *mutationResponseLossPeer) outputText() string {
	p.outputMu.Lock()
	defer p.outputMu.Unlock()
	return p.output.String()
}

func (p *mutationResponseLossPeer) waitLine(want string, timeout time.Duration) (string, error) {
	line, err := p.waitLinePrefix(want, timeout)
	if err != nil {
		return line, err
	}
	if line != want {
		return line, fmt.Errorf("unexpected child marker %q, want %q", line, want)
	}
	return line, nil
}

func (p *mutationResponseLossPeer) waitLinePrefix(prefix string, timeout time.Duration) (string, error) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case line, ok := <-p.lines:
		if !ok {
			return "", fmt.Errorf("child stdout closed (stderr=%q)", p.stderr.String())
		}
		if !strings.HasPrefix(line, prefix) {
			return line, fmt.Errorf("unexpected child marker %q, want prefix %q", line, prefix)
		}
		return line, nil
	case err := <-p.readErr:
		return "", fmt.Errorf("read child stdout: %w", err)
	case <-timer.C:
		return "", fmt.Errorf("timed out waiting for child marker %q", prefix)
	}
}

func (p *mutationResponseLossPeer) killAndWait() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil || p.waited {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	err := p.cmd.Wait()
	p.waited = true
	if err == nil {
		return fmt.Errorf("child exited normally before forced termination")
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return nil
	}
	return err
}

func (p *mutationResponseLossPeer) stop() {
	if p == nil || p.waited {
		return
	}
	_ = p.killAndWait()
}

func mutationResponseLossMembers(v1Endpoint string) []quepaxa.Member {
	members := []quepaxa.Member{
		testMember(warmRestartCluster, "v0", warmRestartV0Token),
		testMember(warmRestartCluster, "v1", warmRestartV1Token),
	}
	if v1Endpoint != "" {
		members[1].PeerURL = "quic://" + v1Endpoint
	}
	return members
}
