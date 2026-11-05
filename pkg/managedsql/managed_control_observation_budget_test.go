package managedsql

import (
	"context"
	"errors"
	"testing"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestManagedGraphHostOperationContextRetainsCallerAndHostLifetime(t *testing.T) {
	lifetime, closeHost := context.WithCancel(context.Background())
	defer closeHost()
	h := &Host{cfg: Config{Plan: operator.V1Alpha2ApplicationInitPlan{ApplicationProtocol: sqlpolicy.ManagedGraphRequestAppendOnlyApplicationProtocol}}, admissionLifetime: lifetime}
	type markerKey struct{}
	marker := &struct{}{}
	caller, cancel := context.WithCancel(context.WithValue(context.Background(), markerKey{}, marker))
	defer cancel()
	operation, release := h.internalRequestContext(caller)
	defer release()
	nested, releaseNested := h.internalRequestContext(operation)
	defer releaseNested()
	if nested.Value(markerKey{}) != marker {
		t.Fatal("nested Host scope lost caller context")
	}
	cancel()
	<-nested.Done()
	if !errors.Is(nested.Err(), context.Canceled) {
		t.Fatalf("caller cancellation was lost: %v", nested.Err())
	}
	hostScope, releaseHost := h.internalRequestContext(context.Background())
	defer releaseHost()
	closeHost()
	<-hostScope.Done()
	if !errors.Is(hostScope.Err(), context.Canceled) {
		t.Fatalf("Host lifetime cancellation was lost: %v", hostScope.Err())
	}
}
