package operator

import (
	"context"
	"errors"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/thanos-io/objstore"
)

func TestPhase1AdmissionOracle(t *testing.T) {
	cmd := exec.Command("sh", "../../scripts/test-ha-v1alpha2-contract.sh", "--self-test-oracle")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("admission oracle self-test: %v\n%s", err, output)
	}
}

type phase1Transport struct{ calls int }

// Any bucket operation dispatches through a nil embedded interface and fails.
type phase1NoBucketCalls struct{ objstore.Bucket }

func (t *phase1Transport) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls++
	return nil, errors.New("unexpected Kubernetes request")
}

func TestPhase1ControllerRunUnavailableBeforeSideEffects(t *testing.T) {
	transport := &phase1Transport{}
	ctx := context.Background()
	c := &Controller{Kube: &Kubernetes{BaseURL: "http://invalid", Namespace: "test", Client: &http.Client{Transport: transport}}, Bucket: &phase1NoBucketCalls{}, Automatic: true}
	if err := c.Run(ctx, time.Second); !errors.Is(err, ErrHARecoveryUnavailable) {
		t.Fatalf("Run error = %v, want unavailable", err)
	}
	if transport.calls != 0 {
		t.Fatalf("Kubernetes requests = %d, want none", transport.calls)
	}
}

func TestPhase1ControllerRunUnavailableWithNilConfiguration(t *testing.T) {
	if err := (&Controller{}).Run(context.Background(), 0); !errors.Is(err, ErrHARecoveryUnavailable) {
		t.Fatalf("Run error = %v, want unavailable", err)
	}
}
