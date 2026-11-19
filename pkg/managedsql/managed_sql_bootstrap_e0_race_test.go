//go:build rhiza_local_testhooks

package managedsql

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestManagedPolicy9OriginalReloadsConsumedE0BeforeFreshSourceCheck(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedRequestAppendOnlyApplicationProtocol)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	entered, resume := make(chan struct{}), make(chan struct{})
	var hold atomic.Bool
	hold.Store(true)
	restore := localtesthooks.Set(func(event string) {
		if event == "managed-host:sql-bootstrap:before-unused-source-check" && hold.Swap(false) {
			close(entered)
			<-resume
		}
	})
	defer restore()
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()

	type openResult struct {
		host *Host
		err  error
	}
	late := make(chan openResult, 1)
	go func() {
		h, err := fixture.open(ctx, configs[2])
		late <- openResult{host: h, err: err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("late original did not reach its pre-source E0 reload", ctx.Err())
	}

	for _, cfg := range configs[:2] {
		h, err := fixture.open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = fixture.closeHost(h) })
	}
	var consumed operator.V1Alpha2ApplicationInitSnapshot
	deadline := time.Now().Add(5 * time.Second)
	for {
		var err error
		consumed, err = operator.LoadV1Alpha2ApplicationInit(ctx, configs[0].Store, configs[0].Ref)
		if err != nil {
			t.Fatal(err)
		}
		if consumed.Assignment().State == "Consumed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first two peers did not consume E0")
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(resume)
	result := <-late
	if result.host != nil {
		t.Cleanup(func() { _ = fixture.closeHost(result.host) })
	}
	if result.err != nil {
		t.Fatalf("late original rejected peer-consumed E0: %v", result.err)
	}
	after, err := operator.LoadV1Alpha2ApplicationInit(ctx, configs[0].Store, configs[0].Ref)
	if err != nil || after.Assignment() != consumed.Assignment() {
		t.Fatalf("late original changed peer-consumed E0: %v", err)
	}
}
