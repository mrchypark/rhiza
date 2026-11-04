package managedsql

import (
	"bytes"
	"log"
	"strings"
	"testing"
)

func TestManagedHostRequestRecoveryPrecheckDiagnosticIsFixed(t *testing.T) {
	oldWriter, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(oldWriter)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	var output bytes.Buffer
	log.SetOutput(&output)
	logManagedHostRequestRecoveryPrecheckFailure()
	line := output.String()
	if !strings.Contains(line, "MANAGED_HOST_REQUEST_RECOVERY_FAILURE stage=host_precheck cause=not_ready") {
		t.Fatalf("Host precheck diagnostic missing fixed stage/cause: %q", line)
	}
	if strings.Contains(line, "request-id") || strings.Contains(line, "secret") {
		t.Fatalf("Host precheck diagnostic leaked private data: %q", line)
	}
}
