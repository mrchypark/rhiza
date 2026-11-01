package managedbootstrap

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/pkg/operator"
)

func TestLoadConfigNeverCreatesMissingIdentityOrHandoffs(t *testing.T) {
	dir := t.TempDir()
	r := ConfigRequest{Ref: operator.V1Alpha2ClusterRef{Name: "sql", Namespace: "ns", UID: "uid"}, PodName: "pod", PodUID: "pod-uid", IdentityPath: filepath.Join(dir, "identity.json"), BootstrapPath: filepath.Join(dir, "bootstrap.json"), RepairPath: filepath.Join(dir, "repair.json")}
	_, cleanup, err := LoadConfig(context.Background(), r)
	if err == nil || cleanup == nil {
		t.Fatal("missing identity was accepted or cleanup contract broken")
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{r.IdentityPath, r.BootstrapPath, r.RepairPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("created startup input %s: %v", path, err)
		}
	}
}
