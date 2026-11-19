package managedsql

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestValidateManagedStartupFreshUsesOneLiveSnapshot(t *testing.T) {
	configs, fixture := sqlCluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	host, err := fixture.open(ctx, configs[0])
	if host != nil {
		t.Cleanup(func() {
			if err := fixture.closeHost(host); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if !host.identity.Fresh {
		t.Fatal("fixture did not exercise the fresh startup path")
	}

	kubernetes := configs[0].Store.Kube
	client := *kubernetes.Client
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	var gets atomic.Int64
	client.Transport = repairGateTransport(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/rhizaclusters/") {
			gets.Add(1)
		}
		return base.RoundTrip(r)
	})
	previous := kubernetes.Client
	kubernetes.Client = &client
	t.Cleanup(func() { kubernetes.Client = previous })

	before := gets.Load()
	if err := host.ValidateManagedStartup(ctx, host.identity); err != nil {
		t.Fatal(err)
	}
	if got := gets.Load() - before; got != 1 {
		t.Fatalf("fresh startup CR state reads=%d, want 1 (single atomic snapshot)", got)
	}
}
