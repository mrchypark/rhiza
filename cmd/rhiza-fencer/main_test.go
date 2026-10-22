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

func TestSourceWriterConfigValidationIsPureAndFailClosed(t *testing.T) {
	tests := []struct {
		name    string
		config  sourceWriterConfig
		wantErr string
	}{
		{
			name: "independent profile accepts explicit ADC with remote stores",
			config: sourceWriterConfig{Independent: true, HasCanonicalGCSState: true,
				RuntimeAgentCA: "/runtime-agent-ca", UseADC: true},
		},
		{
			name: "independent profile preserves credential file mode",
			config: sourceWriterConfig{Independent: true, HasCanonicalGCSState: true,
				RuntimeAgentCA: "/runtime-agent-ca", CredentialsFile: "/writer/credentials.json"},
		},
		{
			name:   "cooperative profile stays valid without source mutation",
			config: sourceWriterConfig{},
		},
		{
			name:    "ADC is not an implicit cooperative mode",
			config:  sourceWriterConfig{HasCanonicalGCSState: true, UseADC: true},
			wantErr: "require the independent mutation profile",
		},
		{
			name: "ADC and credentials file conflict",
			config: sourceWriterConfig{Independent: true, HasCanonicalGCSState: true,
				RuntimeAgentCA: "/runtime-agent-ca", CredentialsFile: "/writer/credentials.json", UseADC: true},
			wantErr: "mutually exclusive",
		},
		{
			name:    "independent profile requires remote canonical state",
			config:  sourceWriterConfig{Independent: true, RuntimeAgentCA: "/runtime-agent-ca", UseADC: true},
			wantErr: "requires RHIZA_OBJSTORE_PROVIDER=gcs and a bucket",
		},
		{
			name:    "independent profile requires runtime CA",
			config:  sourceWriterConfig{Independent: true, HasCanonicalGCSState: true, UseADC: true},
			wantErr: "requires runtime-agent CA",
		},
		{
			name:    "independent profile requires one writer credential mode",
			config:  sourceWriterConfig{Independent: true, HasCanonicalGCSState: true, RuntimeAgentCA: "/runtime-agent-ca"},
			wantErr: "exactly one separate source writer credential mode",
		},
		{
			name: "independent profile rejects local stage state",
			config: sourceWriterConfig{Independent: true, HasCanonicalGCSState: true,
				RuntimeAgentCA: "/runtime-agent-ca", UseADC: true, StageDir: "/stage"},
			wantErr: "remote source staging and receipts",
		},
		{
			name:    "legacy file mode still requires paired local receipts",
			config:  sourceWriterConfig{HasCanonicalGCSState: true, CredentialsFile: "/writer/credentials.json"},
			wantErr: "both a receipt directory and separate writer credentials",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSourceWriterConfig(tt.config)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("valid config rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}
