package node

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
)

func TestManagedGraphReconstructionFailureDiagnostic(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })

	n := &Node{}
	if _, err := n.reconstructManagedGraphStartupAt(context.Background(), nil, nil, nil); err == nil {
		t.Fatal("expected an error without a locked startup")
	}
	if !strings.Contains(output.String(), "MANAGED_GRAPH_RECONSTRUCTION_FAILURE stage=startup_binding") {
		t.Fatalf("missing fixed failure stage: %q", output.String())
	}
}
