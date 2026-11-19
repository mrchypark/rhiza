package managedbootstrap

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestBootstrapHandoffLimitCoversLargePlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap.json")
	data, err := json.Marshal(map[string]string{"payload": strings.Repeat("x", 70<<10)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	var decoded map[string]string
	if err := ReadGateFile(path, &decoded, operator.V1Alpha2BootstrapHandoffLimit, false); err != nil {
		t.Fatalf("large public bootstrap handoff rejected: %v", err)
	}
	if len(decoded["payload"]) != 70<<10 {
		t.Fatal("large bootstrap handoff was truncated")
	}
}
