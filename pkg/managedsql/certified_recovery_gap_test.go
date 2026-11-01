//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

type certifiedRecoveryControlKey struct{}

// Status reads from the real peer-learning path fail while explicit recovery
// still uses the real live authority API. No SQL or consensus result is mocked.
func heldCertifiedRecoveryHosts(t *testing.T) ([]Config, []*Host, *sqlStatusFixture, *atomic.Bool, <-chan struct{}, <-chan struct{}, func()) {
	t.Helper()
	held := &atomic.Bool{}
	configs, hosts, fixture := concurrentSQLHosts(t, func(i int, base http.RoundTripper) http.RoundTripper {
		if i != 1 {
			return base
		}
		return repairGateTransport(func(r *http.Request) (*http.Response, error) {
			if held.Load() && r.Method == http.MethodGet && r.Context().Value(certifiedRecoveryControlKey{}) != true {
				return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":"held peer authority read"}`)), Request: r}, nil
			}
			return base.RoundTrip(r)
		})
	})
	pushSeen, pullSeen, releasePull := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var pushOnce, pullOnce, releaseOnce sync.Once
	target := configs[1].Runtime.NodeID
	restore := localtesthooks.Set(func(event string) {
		if !held.Load() {
			return
		}
		if strings.HasPrefix(event, fmt.Sprintf("network:learned:phase=handler-result:node=%s:", target)) && strings.Contains(event, ":slot=1:") && !strings.HasSuffix(event, `error=""`) {
			pushOnce.Do(func() { close(pushSeen) })
		}
		if strings.HasPrefix(event, fmt.Sprintf("node:catchup-peer:phase=before-accept:node=%s:", target)) && strings.Contains(event, ":from=1:") {
			pullOnce.Do(func() { close(pullSeen) })
			<-releasePull
		}
	})
	release := func() {
		held.Store(false)
		releaseOnce.Do(func() { close(releasePull) })
	}
	// Release the periodic worker before the Host cleanup waits for shutdown.
	t.Cleanup(func() { release(); restore() })
	return configs, hosts, fixture, held, pushSeen, pullSeen, release
}

func TestManagedSQLCertifiedRecoveryGapHoldsPushAndPeriodicPull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	configs, hosts, fixture, held, pushSeen, pullSeen, release := heldCertifiedRecoveryHosts(t)
	ctx = context.WithValue(ctx, certifiedRecoveryControlKey{}, true)
	held.Store(true)
	testManagedSQLSubmittedUnknown(t, ctx, configs, hosts, fixture, pushSeen, pullSeen, release)
}
