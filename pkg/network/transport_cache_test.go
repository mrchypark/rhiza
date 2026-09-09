package network

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

type countedResolver struct {
	testClusterResolver
	copies  atomic.Int32
	staleID uint
}

func (r *countedResolver) CurrentCluster() quepaxa.Cluster {
	r.copies.Add(1)
	return r.current
}
func (r *countedResolver) ClusterForSlot(slot quepaxa.Slot) quepaxa.Cluster {
	r.copies.Add(1)
	return r.testClusterResolver.ClusterForSlot(slot)
}
func (r *countedResolver) ConfigID() uint {
	if r.staleID != 0 {
		return r.staleID
	}
	return r.current.ConfigID
}

func TestTransportCacheCopiesOnlyOnMiss(t *testing.T) {
	old := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "a", Token: "old"}}}
	next := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{{ID: "a", Token: "new"}}}
	r := &countedResolver{testClusterResolver: testClusterResolver{current: next, slots: map[quepaxa.Slot]quepaxa.Cluster{1: old}}}
	transport := NewTransport("cluster", "a", &old, "admin")
	t.Cleanup(func() { _ = transport.Close() })
	transport.BindCore(r)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if _, err := transport.transportForCurrent(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if r.copies.Load() != 1 {
		t.Fatalf("cold concurrent copies=%d", r.copies.Load())
	}
	active, _ := transport.transportForCurrent()
	historical, err := transport.transportForSlot(1)
	if err != nil {
		t.Fatal(err)
	}
	if historical == active || active == transport || historical == transport || historical.token != "old" || active.token != "new" || historical.tls.ClientSessionCache == active.tls.ClientSessionCache || historical.peers["a"] == active.peers["a"] {
		t.Fatal("configuration credentials, pools or TLS sessions were shared")
	}
	if allocs := testing.AllocsPerRun(100, func() {
		_, _ = transport.transportForCurrent()
		_, _ = transport.transportForSlot(1)
	}); allocs != 0 {
		t.Fatalf("cache hit allocations=%g", allocs)
	}
	if r.copies.Load() != 2 {
		t.Fatalf("cache hits copied %d snapshots", r.copies.Load()-2)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := transport.transportForSlot(1); !errors.Is(err, errTransportClosed) {
		t.Fatalf("closed historical lookup: %v", err)
	}
	if r.copies.Load() != 2 {
		t.Fatal("closed transport queried snapshot")
	}
}

func TestTransportCacheUsesSnapshotIDAfterTransition(t *testing.T) {
	old := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "a", Token: "old"}}}
	next := quepaxa.Cluster{ConfigID: 2, Members: []quepaxa.Member{{ID: "a", Token: "new"}}}
	r := &countedResolver{testClusterResolver: testClusterResolver{current: next, slots: map[quepaxa.Slot]quepaxa.Cluster{1: old}}, staleID: 1}
	transport := NewTransport("cluster", "a", &old, "admin")
	defer transport.Close()
	transport.BindCore(r)
	active, err := transport.transportForCurrent()
	if err != nil {
		t.Fatal(err)
	}
	if active.configID != 2 || transport.dynamic[1] != nil || transport.dynamic[2] != active {
		t.Fatal("snapshot cached under stale ID")
	}
	historical, err := transport.transportForSlot(1)
	if err != nil || historical.configID != 1 || historical.token != "old" {
		t.Fatalf("historical routing after race: %v", err)
	}
}
