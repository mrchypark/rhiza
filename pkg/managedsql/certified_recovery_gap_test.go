package managedsql

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mrchypark/rhiza/pkg/operator"
)

type certifiedRecoveryControlKey struct{}

// Status reads from the real peer-learning path fail while explicit recovery
// still uses the real live authority API. No SQL or consensus result is mocked.
func heldCertifiedRecoveryHosts(t *testing.T) ([]Config, []*Host, *sqlStatusFixture, *atomic.Bool) {
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
	t.Cleanup(func() { held.Store(false) })
	return configs, hosts, fixture, held
}

func logCertifiedRecoveryState(t *testing.T, ctx context.Context, hosts []*Host, cfg Config, request ExecuteRequest, stage string) {
	t.Helper()
	for i, h := range hosts {
		state, err := h.node.ManagedState()
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("%s host=%d recorder=%d certified=%d applied=%d uncertain=%t", stage, i, state.RecorderTip, state.CertifiedTip, state.AppliedTip, h.uncertain.Load())
	}
	admission, err := operator.LoadV1Alpha2Admission(ctx, cfg.Store, cfg.Ref)
	if err != nil {
		t.Fatal(err)
	}
	pending, ok := admission.Pending()
	original, originalOK, err := admission.PendingSQL()
	if err != nil {
		t.Fatal(err)
	}
	want, err := requestIdentity(request)
	if err != nil {
		t.Fatal(err)
	}
	originalExact := false
	if originalOK {
		got, identityErr := requestIdentity(original)
		originalExact = identityErr == nil && got == want
	}
	t.Logf("%s pending=%t fingerprint-exact=%t original-exact=%t", stage, ok, ok && pending == want, originalExact)
}
