package main

import (
	"net"
	"strings"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
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
			config: sourceWriterConfig{Independent: true, HasCanonicalState: true, SourceProvider: objectstore.ProviderGCS,
				RuntimeAgentCA: "/runtime-agent-ca", UseADC: true},
		},
		{
			name: "independent profile preserves credential file mode",
			config: sourceWriterConfig{Independent: true, HasCanonicalState: true, SourceProvider: objectstore.ProviderGCS,
				RuntimeAgentCA: "/runtime-agent-ca", CredentialsFile: "/writer/credentials.json"},
		},
		{
			name: "independent profile rejects storage emulator",
			config: sourceWriterConfig{Independent: true, HasCanonicalState: true, SourceProvider: objectstore.ProviderGCS,
				RuntimeAgentCA: "/runtime-agent-ca", UseADC: true, StorageEmulatorHost: "http://127.0.0.1:9999"},
			wantErr: "rejects STORAGE_EMULATOR_HOST",
		},
		{
			name:   "cooperative profile stays valid without source mutation",
			config: sourceWriterConfig{},
		},
		{
			name:   "cooperative profile does not configure a GCS mutation provider",
			config: sourceWriterConfig{StorageEmulatorHost: "http://127.0.0.1:9999"},
		},
		{
			name:    "ADC is not an implicit cooperative mode",
			config:  sourceWriterConfig{HasCanonicalState: true, SourceProvider: objectstore.ProviderGCS, UseADC: true},
			wantErr: "require the independent mutation profile",
		},
		{
			name: "ADC and credentials file conflict",
			config: sourceWriterConfig{Independent: true, HasCanonicalState: true, SourceProvider: objectstore.ProviderGCS,
				RuntimeAgentCA: "/runtime-agent-ca", CredentialsFile: "/writer/credentials.json", UseADC: true},
			wantErr: "mutually exclusive",
		},
		{
			name:    "independent profile requires remote canonical state",
			config:  sourceWriterConfig{Independent: true, SourceProvider: objectstore.ProviderGCS, RuntimeAgentCA: "/runtime-agent-ca", UseADC: true},
			wantErr: "requires a configured remote canonical authority store",
		},
		{
			name:    "independent profile requires runtime CA",
			config:  sourceWriterConfig{Independent: true, HasCanonicalState: true, SourceProvider: objectstore.ProviderGCS, UseADC: true},
			wantErr: "requires runtime-agent CA",
		},
		{
			name:    "independent profile requires one writer credential mode",
			config:  sourceWriterConfig{Independent: true, HasCanonicalState: true, SourceProvider: objectstore.ProviderGCS, RuntimeAgentCA: "/runtime-agent-ca"},
			wantErr: "exactly one separate source writer credential mode",
		},
		{
			name: "independent profile rejects local stage state",
			config: sourceWriterConfig{Independent: true, HasCanonicalState: true, SourceProvider: objectstore.ProviderGCS,
				RuntimeAgentCA: "/runtime-agent-ca", UseADC: true, StageDir: "/stage"},
			wantErr: "remote source staging and receipts",
		},
		{
			name:    "legacy file mode still requires paired local receipts",
			config:  sourceWriterConfig{HasCanonicalState: true, SourceProvider: objectstore.ProviderGCS, CredentialsFile: "/writer/credentials.json"},
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

func TestCanonicalStateSelectionIsProviderNeutralButMutationProfileIsNot(t *testing.T) {
	for _, test := range []struct {
		provider objectstore.Provider
		bucket   string
		want     bool
	}{
		{provider: objectstore.ProviderFilesystem, bucket: "/tmp/rhiza", want: false},
		{provider: objectstore.ProviderS3, bucket: "rhiza", want: true},
		{provider: objectstore.ProviderGCS, bucket: "rhiza", want: true},
		{provider: objectstore.ProviderAzure, bucket: "rhiza", want: true},
	} {
		if got := hasRemoteCanonicalState(objectstore.Config{Provider: test.provider, Bucket: test.bucket}); got != test.want {
			t.Errorf("provider %q canonical state=%t, want %t", test.provider, got, test.want)
		}
	}
	for _, provider := range []objectstore.Provider{objectstore.ProviderS3, objectstore.ProviderAzure} {
		err := validateSourceWriterConfig(sourceWriterConfig{
			Independent: true, HasCanonicalState: true, SourceProvider: provider,
			RuntimeAgentCA: "/runtime-agent-ca", UseADC: true,
		})
		if err == nil || !strings.Contains(err.Error(), "source mutation proof profile for provider") || !strings.Contains(err.Error(), string(provider)) {
			t.Errorf("provider %q source profile error = %v", provider, err)
		}
	}
}

func TestServeRejectsIndependentS3BeforeKubernetesOrAuthorityListener(t *testing.T) {
	for _, key := range []string{"RHIZA_FENCE_ATTESTOR_URL", "RHIZA_FENCE_ATTESTOR_TOKEN_FILE", "RHIZA_FENCE_ATTESTOR_CA_FILE"} {
		t.Setenv(key, "")
	}
	t.Setenv("RHIZA_OBJSTORE_PROVIDER", "s3")
	t.Setenv("RHIZA_OBJSTORE_ENDPOINT", "127.0.0.1:19000")
	t.Setenv("RHIZA_OBJSTORE_BUCKET", "rhiza")
	t.Setenv("RHIZA_OBJSTORE_REGION", "us-east-1")
	t.Setenv("RHIZA_OBJSTORE_ACCESS_KEY", "test-access")
	t.Setenv("RHIZA_OBJSTORE_SECRET_KEY", "test-secret")
	t.Setenv("RHIZA_OBJSTORE_INSECURE", "true")

	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listenAddr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatal(err)
	}

	err = serve(t.Context(), []string{"--tls-cert", "/unused/cert", "--tls-key", "/unused/key",
		"--listen", listenAddr, "--independent-mutation-profile=true"})
	if err == nil || !strings.Contains(err.Error(), `source mutation proof profile for provider "s3" not implemented`) {
		t.Fatalf("independent S3 startup error=%v, want exact unsupported-provider refusal", err)
	}
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		t.Fatalf("unsupported S3 profile started an authority listener: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLegacyCooperativeControlStateIsExplicitlyLimitedToCooperativeOnly(t *testing.T) {
	for _, test := range []struct {
		name        string
		independent bool
		receipts    string
		credentials string
		useADC      bool
		want        bool
	}{
		{name: "cooperative default", want: true},
		{name: "independent profile", independent: true},
		{name: "receipt provider configured", receipts: "/receipts"},
		{name: "writer credential configured", credentials: "/writer.json"},
		{name: "ADC configured", useADC: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := legacyCooperativeControlState(test.independent, test.receipts, test.credentials, test.useADC); got != test.want {
				t.Fatalf("legacy cooperative mode=%t, want %t", got, test.want)
			}
		})
	}
}

func TestExecutorMutationJournalRequirementFollowsExistingIndependentFlag(t *testing.T) {
	independent := newFencingExecutor(nil, nil, true, "", "", false)
	if !independent.RequireMutationJournal || independent.LegacyCooperativeControlState {
		t.Fatalf("independent CLI profile not pinned into executor: %+v", independent)
	}
	cooperative := newFencingExecutor(nil, nil, false, "", "", false)
	if cooperative.RequireMutationJournal || !cooperative.LegacyCooperativeControlState {
		t.Fatalf("cooperative CLI profile unexpectedly requires mutation journal: %+v", cooperative)
	}
}
