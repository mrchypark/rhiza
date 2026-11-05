package rhiza_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNormalImagesExcludeFencerAndLegacyTemplateRequiresExplicitImages(t *testing.T) {
	for _, name := range []string{"Dockerfile", "Dockerfile.operator", "Dockerfile.managed-ffi-contract"} {
		data, err := os.ReadFile(name)
		if err != nil || strings.Contains(string(data), "rhiza-fencer") {
			t.Fatalf("normal image %s still requires a fencer: %v", name, err)
		}
	}
	data, err := os.ReadFile("Dockerfile.fencing-test")
	if err != nil || !strings.Contains(string(data), "rhiza-fencer") {
		t.Fatalf("optional fencing fixture lost its binary: %v", err)
	}
	if _, err := os.Stat("cmd/rhiza-fencer/main.go"); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile("e2e/chaos/votercount_gcs_chaos.yaml.in")
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"HISTORICAL / UNSUPPORTED", "__UNSUPPORTED_LEGACY_HOST_IMAGE__", "__UNSUPPORTED_LEGACY_OPERATOR_IMAGE__"} {
		if !strings.Contains(string(data), marker) {
			t.Fatalf("historical template lacks %s", marker)
		}
	}
	for _, marker := range []string{"__HOST_IMAGE__", "__OPERATOR_IMAGE__"} {
		if strings.Contains(string(data), marker) {
			t.Fatalf("historical template still accepts normal image marker %s", marker)
		}
	}
}

func TestRetiredVoterCountLauncherRefusesBeforeExternalActions(t *testing.T) {
	if output, err := exec.Command("/bin/sh", "-n", "e2e/chaos/run-votercount-gcs-chaos.sh").CombinedOutput(); err != nil {
		t.Fatalf("retired launcher has invalid shell syntax: %v\n%s", err, output)
	}
	dir := t.TempDir()
	trace := filepath.Join(dir, "external-actions")
	for _, command := range []string{"kubectl", "gcloud", "openssl", "python3", "mkdir", "sed"} {
		if err := os.WriteFile(filepath.Join(dir, command), []byte("#!/bin/sh\nprintf '%s\\n' \"$0\" >> \"$RHIZA_EXTERNAL_TRACE\"\nexit 99\n"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	outputDir := filepath.Join(dir, "output-must-not-exist")
	for _, image := range []string{"repo@sha256:" + strings.Repeat("a", 64), "repo:v0.19.5", "repo@sha256:", "repo@sha256:not-a-digest", "repo@sha256:zz", "repo@sha256:" + strings.Repeat("a", 63), "repo@sha256:" + strings.Repeat("g", 64)} {
		for _, mode := range []string{"", "install", "run", "unknown"} {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			args := []string{"e2e/chaos/run-votercount-gcs-chaos.sh"}
			if mode != "" {
				args = append(args, mode)
			}
			cmd := exec.CommandContext(ctx, "/bin/sh", args...)
			cmd.Env = append(os.Environ(), "PATH="+dir, "RHIZA_EXTERNAL_TRACE="+trace, "RHIZA_OUT="+outputDir,
				"RHIZA_RUN_ID=abc12345", "RHIZA_NAMESPACE=ns", "RHIZA_KUBE_CONTEXT=ctx", "RHIZA_GCS_BUCKET=b",
				"RHIZA_GCS_PREFIX=p/", "RHIZA_CLUSTER_ID=cid", "RHIZA_GCP_PROJECT_ID=project", "RHIZA_DATA_GSA_ID=data-gsa",
				"RHIZA_APP_IMAGE="+image, "RHIZA_OPERATOR_IMAGE=repo@sha256:"+strings.Repeat("f", 64))
			output, err := cmd.CombinedOutput()
			cancel()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 2 || !strings.Contains(string(output), "retired before external actions") {
				t.Fatalf("mode=%q image=%q: exit=%v output=%s", mode, image, err, output)
			}
			for _, name := range []string{trace, outputDir} {
				if _, err := os.Stat(name); !os.IsNotExist(err) {
					t.Fatalf("retired launcher created resource %s: %v", name, err)
				}
			}
		}
	}
}
