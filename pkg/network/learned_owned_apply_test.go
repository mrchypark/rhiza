//go:build rhiza_local_testhooks

package network

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnedOwnedApplyLifetimeAndClose(t *testing.T) {
	for _, mode := range []string{"remote-cancel", "peer-cancel", "server-close"} {
		t.Run(mode, func(t *testing.T) {
			wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
			if err != nil {
				t.Fatal(err)
			}
			defer wal.Close()
			core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
			if err != nil {
				t.Fatal(err)
			}
			mat, err := materializer.Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
			if err != nil {
				t.Fatal(err)
			}
			defer mat.Close()
			server := NewServer(core, mat, "test", true, nil)
			defer server.Close()
			peer, stopPeer := context.WithCancel(context.Background())
			defer stopPeer()
			stream, stopStream := context.WithCancel(context.Background())
			defer stopStream()
			stream = context.WithValue(stream, peerStreamLifetimeKey{}, peerStreamLifetime{ctx: peer, deadline: time.Now().Add(10 * time.Second)})
			registered := make(chan struct{})
			var once sync.Once
			restore := localtesthooks.Set(func(event string) {
				if event == "network:learned:owned-registered:node=n1" {
					once.Do(func() { close(registered) })
				}
			})
			defer restore()
			server.applyMu.Lock()
			locked := true
			defer func() {
				if locked {
					server.applyMu.Unlock()
				}
			}()
			done := make(chan error, 1)
			go func() { done <- server.applyLearnedOwned(stream, 0) }()
			select {
			case <-registered:
			case <-time.After(5 * time.Second):
				t.Fatal("registration did not complete")
			}
			var closed chan struct{}
			switch mode {
			case "remote-cancel":
				stopStream()
			case "peer-cancel":
				stopPeer()
			case "server-close":
				closed = make(chan struct{})
				go func() { server.Close(); close(closed) }()
				select {
				case <-server.proposalCtx.Done():
				case <-time.After(5 * time.Second):
					t.Fatal("server shutdown not signaled")
				}
				select {
				case <-closed:
					t.Fatal("Close returned before registered apply drained")
				default:
				}
			}
			server.applyMu.Unlock()
			locked = false
			select {
			case err := <-done:
				if mode == "remote-cancel" && err != nil {
					t.Fatalf("remote cancel interrupted owned work: %v", err)
				}
				if mode != "remote-cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("local shutdown error=%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("owned apply did not finish")
			}
			if closed != nil {
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Fatal("Close did not join")
				}
			}
		})
	}
}

func TestLearnedQualificationMissingPrefixFollowsStreamContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p := newForegroundAPIPeers(t, t.TempDir())
	for _, key := range []string{"first", "second"} {
		if _, err := p.servers["n1"].KVPut(ctx, KVMutationRequest{RequestID: key, Key: key, Value: []byte(key)}); err != nil {
			t.Fatal(err)
		}
	}
	decisions, _, err := p.cores["n1"].DecisionsFrom(2, 1)
	if err != nil || len(decisions) != 1 {
		t.Fatalf("source count=%d err=%v", len(decisions), err)
	}
	var decision quepaxa.Decision
	if err := json.Unmarshal(decisions[0].Certificate, &decision); err != nil {
		t.Fatal(err)
	}
	decision.Proposal.Value = decisions[0].Value
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n2", Cluster: p.config, WAL: wal, Transport: p.transports[1]})
	if err != nil {
		t.Fatal(err)
	}
	mat, err := materializer.Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	server := NewServer(core, mat, "foreground-api", true, nil)
	defer server.Close()
	peer := &PeerServer{server: server}
	request := p.transports[0].request(peerfb.OperationLearned)
	request.Decision = decisionToWire(decision)
	stream, stop := context.WithCancel(ctx)
	defer stop()
	deadline := time.Now().Add(150 * time.Millisecond)
	qualified, finish := learnedQualificationContext(stream, ctx, server.proposalCtx, deadline)
	defer finish()
	qualified = context.WithValue(qualified, peerStreamLifetimeKey{}, peerStreamLifetime{ctx: ctx, deadline: deadline})
	if _, err := peer.handle(qualified, p.config.Members[0].PublicKey[:], request); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing prefix result=%v", err)
	}
	if stream.Err() != nil {
		t.Fatalf("qualification deadline canceled stream: %v", stream.Err())
	}
	if mat.Tip() != 0 {
		t.Fatalf("missing prefix was applied: tip=%d", mat.Tip())
	}
}

func TestLearnedOwnedFinalCancellationPreservesAppliedState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1})
	if slot, _, err := core.Propose(ctx, value); err != nil || slot != 1 {
		t.Fatalf("proposal slot=%d err=%v", slot, err)
	}
	mat, err := materializer.Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer mat.Close()
	server := NewServer(core, mat, "test", true, nil)
	defer server.Close()
	materialEntered, tipEntered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var materialOnce, tipOnce, releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	restore := localtesthooks.Set(func(event string) {
		switch event {
		case "materializer:control:before-sqlite":
			materialOnce.Do(func() { close(materialEntered); <-release })
		case "network:learned:before-material-tip":
			tipOnce.Do(func() { close(tipEntered) })
		}
	})
	defer restore()
	materialDone := make(chan error, 1)
	go func() { materialDone <- mat.Apply(ctx, 1, value) }()
	select {
	case <-materialEntered:
	case <-ctx.Done():
		t.Fatal("competing materializer did not reach gate")
	}
	stream := context.WithValue(ctx, peerStreamLifetimeKey{}, peerStreamLifetime{ctx: ctx, deadline: time.Now().Add(10 * time.Second)})
	ownedDone := make(chan error, 1)
	go func() { ownedDone <- server.applyLearnedOwned(stream, 1) }()
	select {
	case <-tipEntered:
	case <-ctx.Done():
		t.Fatal("owned apply did not reach tip boundary")
	}
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	select {
	case <-server.proposalCtx.Done():
	case <-ctx.Done():
		t.Fatal("shutdown not signaled")
	}
	releaseGate()
	select {
	case err := <-materialDone:
		if err != nil {
			t.Fatalf("competing apply: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("competing apply did not finish")
	}
	select {
	case err := <-ownedDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("owned result after close=%v", err)
		}
	case <-ctx.Done():
		t.Fatal("owned apply did not finish")
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("server close did not join")
	}
	if mat.Tip() != 1 {
		t.Fatalf("successful competing apply lost: tip=%d", mat.Tip())
	}
}

func TestLearnedOwnedApplyErrorPrecedesLaterClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	core, err := quepaxa.New(quepaxa.Config{NodeID: "n1", Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "n1"}}}, WAL: wal})
	if err != nil {
		t.Fatal(err)
	}
	value := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{2})
	if slot, _, err := core.Propose(ctx, value); err != nil || slot != 1 {
		t.Fatalf("proposal slot=%d err=%v", slot, err)
	}
	mat, err := materializer.Open(filepath.Join(t.TempDir(), "state.sqlite"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := mat.Close(); err != nil {
		t.Fatal(err)
	}
	server := NewServer(core, mat, "test", true, nil)
	defer server.Close()
	returned, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	releaseGate := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseGate()
	restore := localtesthooks.Set(func(event string) {
		if event == "network:learned:owned-apply-returned" {
			once.Do(func() { close(returned); <-release })
		}
	})
	defer restore()
	stream := context.WithValue(ctx, peerStreamLifetimeKey{}, peerStreamLifetime{ctx: ctx, deadline: time.Now().Add(10 * time.Second)})
	done := make(chan error, 1)
	go func() { done <- server.applyLearnedOwned(stream, 1) }()
	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatal("apply error not reached")
	}
	closed := make(chan struct{})
	go func() { server.Close(); close(closed) }()
	select {
	case <-server.proposalCtx.Done():
	case <-ctx.Done():
		t.Fatal("close not signaled")
	}
	releaseGate()
	select {
	case err := <-done:
		if !errors.Is(err, sql.ErrConnDone) {
			t.Fatalf("apply error lost after cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("owned apply did not finish")
	}
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("close did not join")
	}
}

func TestLearnedOwnedApplyRejectsCanceledBeforeHandoff(t *testing.T) {
	for _, mode := range []string{"stream", "peer", "deadline", "server"} {
		t.Run(mode, func(t *testing.T) {
			server := NewServer(nil, nil, "test", true, nil)
			defer server.Close()
			peer, cancelPeer := context.WithCancel(context.Background())
			defer cancelPeer()
			stream, cancelStream := context.WithCancel(context.Background())
			defer cancelStream()
			deadline := time.Now().Add(time.Minute)
			switch mode {
			case "stream":
				cancelStream()
			case "peer":
				cancelPeer()
			case "deadline":
				deadline = time.Now().Add(-time.Second)
			case "server":
				server.Close()
			}
			ctx := context.WithValue(stream, peerStreamLifetimeKey{}, peerStreamLifetime{ctx: peer, deadline: deadline})
			err := server.applyLearnedOwned(ctx, 0)
			want := context.Canceled
			if mode == "deadline" {
				want = context.DeadlineExceeded
			}
			if mode == "server" {
				want = ErrNotReady
			}
			if !errors.Is(err, want) {
				t.Fatalf("mode=%s got=%v want=%v", mode, err, want)
			}
		})
	}
}
