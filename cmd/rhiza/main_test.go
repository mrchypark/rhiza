package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestReplicaMembersRequirePinnedIdentityOnlyForLearner(t *testing.T) {
	members := []rhiza.Member{{ID: "n1"}, {ID: "n2"}}
	objectMembers, err := objectReplicaMembers("cluster", members, false)
	if err != nil || len(objectMembers) != 2 || objectMembers[1].ID != "n2" {
		t.Fatalf("object-store members=%+v err=%v", objectMembers, err)
	}
	derived, err := rhiza.NewReplicaMember("cluster", rhiza.Member{ID: "n1", PeerURL: "quic://n1:9090", PublicKey: rhiza.PublicKey{1}})
	if err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(derived.PublicKey[:])
	learnerMembers, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"` + key + `"}]`)
	if err != nil || len(learnerMembers) != 1 || learnerMembers[0].PeerURL != "quic://n1:9090" {
		t.Fatalf("learner members=%+v err=%v", learnerMembers, err)
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"` + key + `","token":"voter-secret"}]`); err == nil {
		t.Fatal("learner accepted a voter token field")
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090"}]`); err == nil {
		t.Fatal("learner accepted a member without a public key")
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"` + key + `"}] trailing`); err == nil {
		t.Fatal("learner accepted trailing replica member input")
	}
	historyMembers, err := objectReplicaMembers("cluster", []rhiza.Member{{ID: "n1", URL: "http://n1", PeerURL: "quic://n1:9090", PublicKey: rhiza.PublicKey{1}}}, true)
	if err != nil || len(historyMembers) != 1 || historyMembers[0].PeerURL != "quic://n1:9090" || historyMembers[0].PublicKey != ([32]byte{1}) {
		t.Fatalf("history-aware members=%+v err=%v", historyMembers, err)
	}
}

func TestRunReturnsConfigurationErrors(t *testing.T) {
	t.Setenv("RHIZA_ROLE", "unknown")
	t.Setenv("RHIZA_OBJSTORE_INSECURE", "")
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid RHIZA_ROLE") {
		t.Fatalf("error=%v", err)
	}
}

func TestPrepareRestoreFlagUsesOfflineAPIAndRejectsUnknownFlags(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "must-not-be-created")
	t.Setenv("RHIZA_ROLE", "learner")
	t.Setenv("RHIZA_CLUSTER_ID", "prepare-test")
	t.Setenv("RHIZA_NODE_ID", "fresh-learner")
	t.Setenv("RHIZA_DATA_DIR", dataDir)
	t.Setenv("RHIZA_PREPARED_POD_UID", "")
	t.Setenv("RHIZA_ENABLE_RECONFIGURATION", "true")
	t.Setenv("RHIZA_CLUSTER_MEMBERS", `[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="},{"node_id":"n2","peer_url":"quic://n2:9090","public_key":"AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI="}]`)
	t.Setenv("RHIZA_LEARNER", `{"node_id":"fresh-learner","peer_url":"quic://fresh-learner:9090","public_key":"AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM="}`)
	t.Setenv("RHIZA_REPLICA_MEMBERS", "")

	err := runArgs(context.Background(), []string{"--prepare-restore"})
	if err == nil || !strings.Contains(err.Error(), "Pod UID") {
		t.Fatalf("--prepare-restore error=%v; want the PrepareRestore API's missing Pod UID refusal", err)
	}
	if strings.Contains(err.Error(), "RHIZA_REPLICA_MEMBERS") || strings.Contains(err.Error(), "invalid RHIZA_ROLE") {
		t.Fatalf("prepare mode entered ordinary role startup: %v", err)
	}
	if _, statErr := os.Stat(dataDir); !os.IsNotExist(statErr) {
		t.Fatalf("prepare validation created DataDir: stat error=%v", statErr)
	}

	if err := runArgs(context.Background(), []string{"--prepare-restore", "--unknown-option"}); err == nil {
		t.Fatal("unknown prepare-mode flag was accepted")
	}
	if _, statErr := os.Stat(dataDir); !os.IsNotExist(statErr) {
		t.Fatalf("unknown flag path created DataDir: stat error=%v", statErr)
	}
}

func TestHelpFlagsPrintUsageAndExitSuccessfully(t *testing.T) {
	for _, arg := range []string{"-h", "--help"} {
		t.Run(arg, func(t *testing.T) {
			var output bytes.Buffer
			if err := runArgsWithOutput(context.Background(), []string{arg}, &output); err != nil {
				t.Fatalf("help returned error: %v", err)
			}
			for _, want := range []string{"Usage of rhiza:", "-prepare-restore", "-enroll-existing-voter"} {
				if !strings.Contains(output.String(), want) {
					t.Fatalf("help output %q does not contain %q", output.String(), want)
				}
			}
		})
	}
}

func TestClusterMemberJSONRejectsTypos(t *testing.T) {
	var members []rhiza.Member
	if err := decodeStrictJSON(`[{"node_id":"n1","peer_ur":"quic://n1"}]`, &members); err == nil {
		t.Fatal("unknown cluster member field was accepted")
	}
}

func TestServerBinaryUsesOnlyRootRhizaAPI(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	mainFile := filepath.Join(filepath.Dir(file), "main.go")
	parsed, err := parser.ParseFile(token.NewFileSet(), mainFile, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range parsed.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(path, "github.com/mrchypark/rhiza/") {
			t.Fatalf("server bypasses the root embedded API with import %q", path)
		}
	}
}
