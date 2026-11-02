//go:build cgo

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mrchypark/rhiza/internal/peertest"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/node"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/quic-go/quic-go"
)

func TestManagedStartupBudgetRetiresWithoutCancelingLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lifetime, closeHandle := context.WithCancel(context.Background())
		defer closeHandle()
		opening, finish := managedStartupContext(lifetime, time.Second)
		child, closeChild := context.WithCancel(opening) // Node.openInternal's actual lineage.
		defer closeChild()
		if err := finish(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if child.Err() != nil {
			t.Fatal("successful open canceled the runtime or left its startup timer armed")
		}
		if err := finish(); err != nil || child.Err() != nil {
			t.Fatal("repeated completion canceled runtime")
		}
		closeHandle()
		synctest.Wait()
		if !errors.Is(child.Err(), context.Canceled) {
			t.Fatal("handle cancellation did not reach runtime")
		}
	})
}

func TestManagedStartupBudgetTimeoutCannotBecomeSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opening, finish := managedStartupContext(context.Background(), time.Second)
		time.Sleep(time.Second)
		synctest.Wait()
		if opening.Err() == nil || !errors.Is(finish(), context.DeadlineExceeded) || !errors.Is(finish(), context.DeadlineExceeded) {
			t.Fatal("expired startup was reported successful")
		}
	})
}

func TestManagedStartupBudgetParentCancellation(t *testing.T) {
	lifetime, cancel := context.WithCancel(context.Background())
	opening, finish := managedStartupContext(lifetime, time.Hour)
	cancel()
	if !errors.Is(finish(), context.Canceled) || opening.Err() == nil {
		t.Fatal("startup ignored handle cancellation")
	}
}

func TestManagedStartupBudgetCompletionDeadlineRace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for range 32 {
			parent, cancel := context.WithCancel(context.Background())
			opening, finish := managedStartupContext(parent, time.Second)
			result := make(chan error, 1)
			go func() { time.Sleep(time.Second); result <- finish() }()
			time.Sleep(time.Second)
			err := <-result
			synctest.Wait()
			if err == nil && opening.Err() != nil {
				t.Fatal("startup success returned with an already canceled runtime")
			}
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unexpected startup race outcome: %v", err)
			}
			cancel()
		}
	})
}

func TestManagedStartupBudgetKeepsRealCorePeerUntilClose(t *testing.T) {
	lifetime, closeHandle := context.WithCancel(context.Background())
	defer closeHandle()
	opening, finish := managedStartupContext(lifetime, 30*time.Second)
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport := &quic.Transport{Conn: conn}
	t.Cleanup(func() {
		if err := transport.Close(); err != nil {
			t.Error(err)
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	addr := conn.LocalAddr().String()
	assertHeld := func() {
		t.Helper()
		other, err := net.ListenPacket("udp", addr)
		if err == nil {
			_ = other.Close()
			t.Fatal("managed FFI context fixture released endpoint")
		}
	}
	assertHeld()
	cluster, id, token := types.ClusterID("ffi-context"), types.NodeID("ffi-peer"), "ffi-context-peer-token"
	opening = peertest.WithTransport(opening, string(cluster), string(id), addr, transport)
	member := quepaxa.Member{ID: id, PublicKey: quepaxa.PublicKey(network.PeerPublicKey(cluster, id, token)), PeerURL: "quic://" + addr}
	actual := node.New(&types.ExecutionConfig{DataDir: t.TempDir(), ClusterID: cluster, NodeID: id, Members: []quepaxa.Member{member}, PeerAddr: addr, PeerToken: token})
	if err := actual.Open(opening); err != nil {
		_ = finish()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := actual.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	if err := finish(); err != nil {
		t.Fatal(err)
	}
	client := network.NewTransport(cluster, id, &quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{member}}, token)
	t.Cleanup(func() { _ = client.Close() })
	request, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := client.ReadTip(request, id); err != nil {
		t.Fatalf("actual Core/WAL private peer died when open returned: %v", err)
	}
	response := registerEntry(&ffiEntry{ctx: lifetime, cancel: closeHandle, cleanup: actual.Shutdown, drained: make(chan struct{})})
	var result struct {
		Data struct {
			Handle uint64 `json:"handle"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response, &result); err != nil || result.Data.Handle == 0 {
		t.Fatal("failed to register owned runtime lifetime")
	}
	requireData(t, mustResponse(t, goClose(result.Data.Handle)))
	assertHeld()
	if opening.Err() == nil {
		t.Fatal("close did not cancel actual runtime context")
	}
}
