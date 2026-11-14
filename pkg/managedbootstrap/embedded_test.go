package managedbootstrap

import (
	"context"
	"testing"
)

func TestOpenEmbeddedRequiresApplicationReadiness(t *testing.T) {
	if _, err := OpenEmbedded(context.Background(), ConfigRequest{}, nil); err == nil {
		t.Fatal("missing application readiness must fail before opening")
	}
}
