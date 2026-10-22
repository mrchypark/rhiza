package main

import (
	"os"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestOptInCandidatePublicationMatchesRemoteGCSRuntimeImages(t *testing.T) {
	workflowData, err := os.ReadFile("../../.github/workflows/ci.yml")
	if err != nil {
		t.Fatal(err)
	}
	type step struct {
		Name string `yaml:"name"`
		If   string `yaml:"if"`
		Run  string `yaml:"run"`
		Uses string `yaml:"uses"`
		With struct {
			Name      string `yaml:"name"`
			Path      string `yaml:"path"`
			Labels    string `yaml:"labels"`
			Tags      string `yaml:"tags"`
			Load      string `yaml:"load"`
			BuildArgs string `yaml:"build-args"`
		} `yaml:"with"`
	}
	type job struct {
		If    string            `yaml:"if"`
		Env   map[string]string `yaml:"env"`
		Needs []string          `yaml:"needs"`
		Steps []step            `yaml:"steps"`
	}
	var workflow struct {
		Jobs map[string]job `yaml:"jobs"`
	}
	if err := yaml.Unmarshal(workflowData, &workflow); err != nil {
		t.Fatal(err)
	}
	containers := workflow.Jobs["containers"]
	var export, artifact *step
	builds := 0
	for i := range containers.Steps {
		candidate := &containers.Steps[i]
		if candidate.Uses == "docker/build-push-action@v6" {
			builds++
			if candidate.With.Load != "true" ||
				!strings.Contains(candidate.With.Labels, "org.opencontainers.image.revision=${{ github.sha }}") ||
				!strings.Contains(candidate.With.Labels, "io.rhiza.candidate.source-sha=${{ github.sha }}") {
				t.Fatal("candidate images must be loadable for exact recovery testing and labeled with their source revision")
			}
			if strings.Contains(candidate.With.Tags, "rhiza:ci") &&
				!strings.Contains(candidate.With.BuildArgs, "RHIZA_APPLICATION_PACKAGE=./e2e/chaos/embedded-go") {
				t.Fatal("the published voter image must build the embedded Go application with its GCS qualification commands")
			}
		}
		if candidate.Name == "Export voter and operator images for exact recovery testing" {
			export = candidate
		}
		if strings.HasPrefix(candidate.Uses, "actions/upload-artifact@") && strings.Contains(candidate.With.Name, "runtime-candidate-") {
			artifact = candidate
		}
	}
	if builds != 2 {
		t.Fatalf("expected the existing app and operator image builds, got %d", builds)
	}
	if export == nil || export.If != "" ||
		!strings.Contains(export.Run, "docker save rhiza:ci rhiza-operator:ci") ||
		!strings.Contains(export.Run, "rhiza-runtime-agent") ||
		!strings.Contains(export.Run, "rhiza-entrypoint") || !strings.Contains(export.Run, "SHA256SUMS") {
		t.Fatal("CI must export checksummed voter and operator images, including the tested entrypoint and runtime agent")
	}
	if artifact == nil || artifact.If != "" ||
		!strings.Contains(artifact.With.Name, "${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}") {
		t.Fatal("runtime image artifact must be available to exact-image recovery tests and bound to commit, run, and attempt")
	}
	publisher := workflow.Jobs["publish-chaos-image"]
	if !strings.Contains(publisher.If, "inputs.publish_chaos_image") || !strings.Contains(publisher.If, "!inputs.fixture_only") ||
		!strings.Contains(publisher.If, "!inputs.fencing_only") || !slices.Contains(publisher.Needs, "containers") ||
		!slices.Contains(publisher.Needs, "automatic-operator-chaos") || !slices.Contains(publisher.Needs, "cooperative-fencing") {
		t.Fatal("candidate publication must remain opt-in and wait for the exact Go recovery and cooperative runtime tests")
	}
	var verify, publish *step
	for i := range publisher.Steps {
		candidate := &publisher.Steps[i]
		if candidate.Name == "Verify and load tested voter and operator images" {
			verify = candidate
		}
		if candidate.Name == "Publish only the immutable run tag" {
			publish = candidate
		}
	}
	if verify == nil || !strings.Contains(verify.Run, "sha256sum -c SHA256SUMS") ||
		!strings.Contains(verify.Run, "docker load -i runtime-images.tar") || !strings.Contains(verify.Run, "cmp \"bin/$binary\"") ||
		!strings.Contains(verify.Run, "bin/rhiza-entrypoint") {
		t.Fatal("publisher must verify the exact CI artifact and embedded runtime binaries before tagging")
	}
	if publish == nil || !strings.Contains(publish.Run, "rhiza-application:ci-$GITHUB_SHA-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT") ||
		!strings.Contains(publish.Run, "rhiza-operator:ci-$GITHUB_SHA-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT") ||
		!strings.Contains(publish.Run, "docker manifest inspect") || !strings.Contains(publish.Run, "rhiza-sql:ci-$GITHUB_SHA-$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT") {
		t.Fatal("publication must retain the legacy host and add immutable app/operator candidate tags")
	}

	for path, required := range map[string][]string{
		"../../Dockerfile":                             {"COPY --from=builder /app/rhiza /usr/local/bin/rhiza", "COPY --from=builder /app/rhiza-fencer /usr/local/bin/rhiza-fencer"},
		"../../Dockerfile.operator":                    {"COPY --from=builder /app/rhiza-operator /usr/local/bin/rhiza-operator", "COPY --from=builder /app/rhiza-fencer /usr/local/bin/rhiza-fencer", "COPY --from=builder /app/rhiza-runtime-agent /usr/local/bin/rhiza-runtime-agent"},
		"../../e2e/chaos/votercount_gcs_chaos.yaml.in": {"command: [/usr/local/bin/rhiza-fencer]", "args: [supervise, --, /usr/local/bin/rhiza]", "image: __HOST_IMAGE__", "image: __OPERATOR_IMAGE__"},
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, fragment := range required {
			if !strings.Contains(string(data), fragment) {
				t.Fatalf("%s no longer supplies runtime contract fragment %q", path, fragment)
			}
		}
	}
	appDockerfile, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(appDockerfile), "ARG RHIZA_APPLICATION_PACKAGE=./cmd/rhiza") ||
		!strings.Contains(string(appDockerfile), "go build -o rhiza \"$RHIZA_APPLICATION_PACKAGE\"") ||
		!strings.Contains(string(appDockerfile), "COPY --from=builder /app/rhiza /usr/local/bin/rhiza-entrypoint") {
		t.Fatal("Dockerfile must preserve the root CLI default while exposing the selected app binary at both runtime paths")
	}
	automatic := workflow.Jobs["automatic-operator-chaos"]
	if !slices.Contains(automatic.Needs, "containers") {
		t.Fatal("automatic recovery must wait for the candidate image artifact")
	}
	var download, load *step
	for i := range automatic.Steps {
		candidate := &automatic.Steps[i]
		if strings.HasPrefix(candidate.Uses, "actions/download-artifact@") && candidate.If == "matrix.host == 'go'" {
			download = candidate
		}
		if candidate.Name == "Load exact Go runtime candidate images" && candidate.If == "matrix.host == 'go'" {
			load = candidate
		}
	}
	if download == nil || !strings.Contains(download.With.Name, "runtime-candidate-${{ github.sha }}-${{ github.run_id }}-${{ github.run_attempt }}") ||
		load == nil || !strings.Contains(load.Run, "sha256sum -c SHA256SUMS") ||
		!strings.Contains(load.Run, "docker load -i runtime-images.tar") ||
		!strings.Contains(load.Run, "docker tag rhiza:ci rhiza-chaos:local") ||
		!strings.Contains(load.Run, "docker tag rhiza-operator:ci rhiza-operator-chaos:local") ||
		!strings.Contains(load.Run, "cmp bin/rhiza rhiza") {
		t.Fatal("the Go automatic-recovery job must test the checksummed exact candidate images, not rebuild a close-enough binary")
	}
	embedded, err := os.ReadFile("../../e2e/chaos/embedded-go/main.go")
	if err != nil || !strings.Contains(string(embedded), "qualification-gcs-iam-probe") {
		t.Fatal("embedded Go application source must include the qualification command required by the GCS workload")
	}
	driver, err := os.ReadFile("../../e2e/chaos/votercount_gcs_chaos.py")
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{`"runtime-agent": self.args.operator_image`, `"runtime-agent": self.pods_with("app.kubernetes.io/name=rhiza-runtime-agent")`} {
		if !strings.Contains(string(driver), fragment) {
			t.Fatalf("remote GCS runtime image inventory must verify the runtime-agent digest: missing %q", fragment)
		}
	}
}
