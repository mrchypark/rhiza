package main

import (
	"strings"
	"testing"
)

func TestQualificationHostContainsPinnedKubectlTools(t *testing.T) {
	dockerfile := readContractFile(t, "../Dockerfile")
	workflow := readContractFile(t, "../../../.github/workflows/ci.yml")
	for _, want := range []string{
		"FROM registry.k8s.io/kubectl@sha256:9cd9070def4060229de73314aef753caff194eebe2ca1f04d21f22320932c0c0 AS kubectl",
		"COPY --from=kubectl /bin/kubectl /usr/local/bin/kubectl",
	} {
		if !strings.Contains(dockerfile, want) {
			t.Errorf("qualification host Dockerfile missing pinned kubectl input %q", want)
		}
	}
	if !strings.Contains(workflow, "docker run --rm --entrypoint /bin/sh rhiza-chaos:local -ec 'command -v kubectl >/dev/null && command -v date >/dev/null && command -v sleep >/dev/null && kubectl version --client --output=yaml'") {
		t.Fatal("CI must smoke-check kubectl, shell utilities, and client version inside the qualification host")
	}
}
