package managedsql

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
)

// Explicit original genesis is allowed without inventing a checkpoint. A later
// registered restart is not that first genesis and cannot bypass required roots.
func TestManagedGraphRegisteredRestartRequiresProducerRoot(t *testing.T) {
	configs, fixture := sqlClusterForProtocol(t, sqlpolicy.ManagedGraphRequestApplicationProtocol)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	first, err := fixture.open(ctx, configs[0])
	if first != nil {
		t.Cleanup(func() { _ = first.Close() })
	}
	if err != nil {
		t.Fatalf("explicit fresh public genesis was refused: %v", err)
	}
	// Do not admit, publish, or submit a mutation: this is specifically the
	// registered-but-no-producer-root boundary, not a recovery success fixture.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := fixture.open(ctx, configs[0])
	if reopened != nil {
		_ = reopened.Close()
	}
	if err == nil || reopened != nil || !strings.Contains(err.Error(), "requires an authenticated producer root") {
		t.Fatalf("registered restart reinterpreted root absence as genesis: %v", err)
	}
}
