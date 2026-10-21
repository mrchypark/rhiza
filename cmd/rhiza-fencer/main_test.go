package main

import (
	"strings"
	"testing"
)

func TestServeRejectsLegacyAttestorBeforeOpeningCredentialsOrListener(t *testing.T) {
	for _, key := range []string{"RHIZA_FENCE_ATTESTOR_URL", "RHIZA_FENCE_ATTESTOR_TOKEN_FILE", "RHIZA_FENCE_ATTESTOR_CA_FILE"} {
		t.Run(key, func(t *testing.T) {
			for _, variable := range []string{"RHIZA_FENCE_ATTESTOR_URL", "RHIZA_FENCE_ATTESTOR_TOKEN_FILE", "RHIZA_FENCE_ATTESTOR_CA_FILE"} {
				t.Setenv(variable, "")
			}
			t.Setenv(key, "test-only-legacy-setting")
			if err := serve(t.Context(), nil); err == nil || !strings.Contains(err.Error(), "legacy v1 quiet-window") {
				t.Fatalf("unsupported profile was not rejected at startup: %v", err)
			}
		})
	}
}
