package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPhase9InitializerRejectsLegacyAutomaticRecoveryBeforeKubernetes(t *testing.T) {
	t.Setenv("RHIZA_AUTOMATIC_RECOVERY", "true")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	err := run(context.Background(), "ns", time.Second, "")
	if err == nil || !strings.Contains(err.Error(), "legacy automatic recovery") {
		t.Fatalf("unexpected error: %v", err)
	}
}
