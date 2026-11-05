//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

type certifiedRecoveryControlKey struct{}

// Hold actual Learned delivery before certificate acceptance and the actual
// periodic certificate pull. No Kubernetes dependency, SQL, or result is mocked.
func heldCertifiedRecoveryHosts(t *testing.T) ([]Config, []*Host, *sqlStatusFixture, *atomic.Bool, <-chan struct{}, <-chan struct{}, func()) {
	t.Helper()
	held := &atomic.Bool{}
	configs, hosts, fixture := concurrentSQLHosts(t, nil)
	pushSeen, pullSeen, releasePull := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var pushOnce, pullOnce, releaseOnce sync.Once
	target := configs[1].Runtime.NodeID
	restore := localtesthooks.Set(func(event string) {
		if !held.Load() {
			return
		}
		if strings.HasPrefix(event, fmt.Sprintf("network:learned:phase=received:node=%s:", target)) {
			pushOnce.Do(func() { close(pushSeen) })
			<-releasePull
		}
		if strings.HasPrefix(event, fmt.Sprintf("node:catchup-peer:phase=before-accept:node=%s:", target)) {
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
