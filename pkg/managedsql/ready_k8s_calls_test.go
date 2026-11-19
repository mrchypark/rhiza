package managedsql

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
)

type countingKubeRoundTripper struct {
	base http.RoundTripper
	gets atomic.Int64
}

func (c *countingKubeRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodGet {
		c.gets.Add(1)
	}
	return c.base.RoundTrip(r)
}

func TestManagedHostMinimizesK8sAPIGetsForLiveStartupAndReadiness(t *testing.T) {
	configs, f := sqlCluster(t)
	(&sqlWireCloseLifecycle{}).observePUTFailures(t, f)

	counter := &countingKubeRoundTripper{base: configs[0].Store.Kube.Client.Transport}
	configs[0].Store.Kube.Client.Transport = counter

	ctx := context.Background()
	hosts := make([]*Host, len(configs))
	openMember := func(i int) {
		h, err := f.open(ctx, configs[i])
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
	}

	openMember(0)

	counter.gets.Store(0)
	if _, err := hosts[0].live(ctx); err != nil {
		t.Fatalf("live: %v", err)
	}
	if got := counter.gets.Load(); got != 1 {
		t.Fatalf("Host.live GETs=%d, want 1", got)
	}

	counter.gets.Store(0)
	if err := hosts[0].ValidateManagedStartup(ctx, hosts[0].identity); err != nil {
		t.Fatalf("ValidateManagedStartup: %v", err)
	}
	if got := counter.gets.Load(); got != 1 {
		t.Fatalf("Host.ValidateManagedStartup GETs=%d, want 1", got)
	}

	for i := 1; i < len(configs); i++ {
		openMember(i)
	}
	for _, h := range hosts {
		readySQL(t, h)
	}

	counter.gets.Store(0)
	if _, err := hosts[0].live(ctx); err != nil {
		t.Fatalf("ready live: %v", err)
	}
	if got := counter.gets.Load(); got != 1 {
		t.Fatalf("ready Host.live GETs=%d, want 1", got)
	}

	counter.gets.Store(0)
	for i := range 5 {
		if !hosts[0].node.Ready() {
			t.Fatalf("node.Ready() probe %d returned false", i)
		}
		if err := hosts[0].Ready(ctx); err != nil {
			t.Fatalf("Host.Ready probe %d failed: %v", i, err)
		}
	}
	if got := counter.gets.Load(); got != 0 {
		t.Fatalf("admitted readiness probes made %d K8s GETs, want 0", got)
	}
}
