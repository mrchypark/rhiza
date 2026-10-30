package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza"
	"github.com/mrchypark/rhiza/internal/types"
)

func TestPrepareIdentityCommand(t *testing.T) {
	dir := t.TempDir()
	identity, report := filepath.Join(dir, "identity"), filepath.Join(dir, "message")
	args := []string{"prepare-identity", "--cr-uid", "cr", "--generation", "0", "--pod-uid", "pod", "--identity-file", identity, "--termination-message", report}
	if err := run(args); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := os.ReadFile(identity)
	if err != nil {
		t.Fatal(err)
	}
	var stored identityFile
	if err := json.Unmarshal(secret, &stored); err != nil {
		t.Fatal(err)
	}
	cluster, _ := types.ManagedClusterID("cr", 0)
	node, _ := types.ManagedNodeID("pod")
	var public struct {
		Version   int             `json:"version"`
		PublicKey rhiza.PublicKey `json:"publicKey"`
	}
	if err := json.Unmarshal(first, &public); err != nil {
		t.Fatal(err)
	}
	if public.Version != 1 || public.PublicKey != rhiza.PeerPublicKey(string(cluster), string(node), stored.Token) || bytes.Contains(first, []byte(stored.Token)) || len(first) > 256 {
		t.Fatal("public report mismatch or secret leak")
	}
	if err := run(args); err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(report)
	if err != nil || !bytes.Equal(first, second) {
		t.Fatal("retry changed public identity")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 2 {
		t.Fatal("unexpected runtime or WAL files")
	}
}

func TestPrepareIdentityCommandRejectsInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{}, {"await-bootstrap-plan"}, {"prepare-identity", "--bad"}, {"prepare-identity", "--cr-uid", "", "--generation", "0", "--pod-uid", "pod"}, {"prepare-identity", "--cr-uid", "cr", "--generation", "-1", "--pod-uid", "pod"}} {
		if err := run(args); err == nil {
			t.Fatalf("accepted invalid arguments %q", args)
		}
	}
}
