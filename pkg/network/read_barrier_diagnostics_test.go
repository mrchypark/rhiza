package network

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type fixedReadTipTransport struct{ unavailableTransport }

func (fixedReadTipTransport) ReadTip(context.Context, quepaxa.NodeID) (quepaxa.Slot, error) {
	return 1, nil
}

func TestReadBarrierFailureLogsReadIndexPhase(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}, {ID: "n2"}}, nil, unavailableTransport{})
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, types.ClusterID("cluster"), true, nil)
	t.Cleanup(func() { server.Close() })

	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = server.Query(ctx, QueryRequest{SQL: "SELECT private_value", Consistency: "linearizable"})
	if !errors.Is(err, quepaxa.ErrQuorumUnavailable) {
		t.Fatalf("Query error = %v, want quorum unavailable", err)
	}
	message := output.String()
	for _, want := range []string{"phase=read-index", "node=n1", "read_index=0", "core_tip=0", "applied_tip=0", "elapsed=", "error_class=quorum-unavailable"} {
		if !strings.Contains(message, want) {
			t.Fatalf("failure log %q does not contain %q", message, want)
		}
	}
	if strings.Contains(message, "private_value") {
		t.Fatalf("failure log leaked query text: %q", message)
	}
}

func TestReadDurabilityFailureLogsReadDurabilityPhase(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, types.ClusterID("cluster"), true, nil)
	t.Cleanup(func() { server.Close() })
	server.SetReadDurabilityBarrier(func(context.Context, quepaxa.Slot) error {
		return errors.New("controlled durability failure")
	})

	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	if _, err := server.Query(context.Background(), QueryRequest{SQL: "SELECT 1", Consistency: "local"}); err == nil {
		t.Fatal("Query succeeded despite controlled durability failure")
	}
	message := output.String()
	for _, want := range []string{"phase=read-durability", "node=n1", "read_index=0", "core_tip=0", "applied_tip=0", "elapsed=", "error_class=durability-unavailable"} {
		if !strings.Contains(message, want) {
			t.Fatalf("failure log %q does not contain %q", message, want)
		}
	}
	if strings.Contains(message, "controlled durability failure") {
		t.Fatalf("failure log leaked backend error text: %q", message)
	}
}

func TestReadBarrierFailureLogsCatchUpPhaseWithoutPeerDetails(t *testing.T) {
	_, values := catchUpValues(t, 1)
	peer := startCatchUpPeer(t, "a", values, func(_ context.Context, req *peerfb.RequestT) error {
		if req.From > 0 {
			return errors.New("private peer payload")
		}
		return nil
	})
	members := []quepaxa.Member{{ID: "n1"}, peer.member}
	cluster := &quepaxa.Cluster{ConfigID: 1, Members: members}
	transport := adminTransport("cluster", "n1", cluster, "admin")
	t.Cleanup(func() { _ = transport.Close() })
	core := mustCore(t, "n1", members, nil, fixedReadTipTransport{})
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, types.ClusterID("cluster"), true, transport)
	t.Cleanup(func() { server.Close() })

	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = server.Query(ctx, QueryRequest{SQL: "SELECT private_query_value", Consistency: "linearizable"})
	if err == nil {
		t.Fatal("Query succeeded despite controlled catch-up failure")
	}
	message := output.String()
	for _, want := range []string{"phase=read-index-catch-up", "node=n1", "read_index=1", "source=a", "error_class="} {
		if !strings.Contains(message, want) {
			t.Fatalf("failure log %q does not contain %q", message, want)
		}
	}
	for _, secret := range []string{"private_query_value", "private peer payload"} {
		if strings.Contains(message, secret) {
			t.Fatalf("failure log leaked %q: %q", secret, message)
		}
	}
}

func TestReadBarrierFailureLogsApplyPhaseWithoutSQLDetails(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	if _, _, err := core.Propose(context.Background(), policySQL(t, "SELECT * FROM private_table_name")); err != nil {
		t.Fatal(err)
	}
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = material.Close() })
	server := NewServer(core, material, types.ClusterID("cluster"), true, nil)
	t.Cleanup(func() { server.Close() })

	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	_, err = server.Query(context.Background(), QueryRequest{SQL: "SELECT private_query_value", Consistency: "linearizable"})
	if err == nil {
		t.Fatal("Query succeeded despite controlled apply failure")
	}
	message := output.String()
	for _, want := range []string{"phase=read-index-apply", "node=n1", "read_index=1", "error_class=backend"} {
		if !strings.Contains(message, want) {
			t.Fatalf("failure log %q does not contain %q", message, want)
		}
	}
	for _, secret := range []string{"private_query_value", "private_table_name", "no such table"} {
		if strings.Contains(message, secret) {
			t.Fatalf("failure log leaked %q: %q", secret, message)
		}
	}
}
