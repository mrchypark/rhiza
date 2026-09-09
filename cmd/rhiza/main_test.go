package main

import (
	"context"
	"encoding/base64"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/mrchypark/rhiza"
)

func TestReplicaMembersRequirePinnedIdentityOnlyForLearner(t *testing.T) {
	members := []rhiza.Member{{ID: "n1"}, {ID: "n2"}}
	objectMembers, err := objectReplicaMembers(members)
	if err != nil || len(objectMembers) != 2 || objectMembers[1].ID != "n2" {
		t.Fatalf("object-store members=%+v err=%v", objectMembers, err)
	}
	derived, err := rhiza.NewReplicaMember("cluster", rhiza.Member{ID: "n1", PeerURL: "quic://n1:9090", Token: "voter-secret"})
	if err != nil {
		t.Fatal(err)
	}
	key := base64.StdEncoding.EncodeToString(derived.PublicKey[:])
	learnerMembers, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"`+key+`"}]`, members)
	if err != nil || len(learnerMembers) != 1 || learnerMembers[0].PeerURL != "quic://n1:9090" {
		t.Fatalf("learner members=%+v err=%v", learnerMembers, err)
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"`+key+`","token":"voter-secret"}]`, members); err == nil {
		t.Fatal("learner accepted voter token")
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"`+key+`"}]`, []rhiza.Member{{ID: "n1", Token: "voter-secret"}}); err == nil {
		t.Fatal("learner accepted voter token in cluster members")
	}
	if _, err := learnerReplicaMembers(`[{"node_id":"n1","peer_url":"quic://n1:9090","public_key":"`+key+`"}] trailing`, members); err == nil {
		t.Fatal("learner accepted trailing replica member input")
	}
}

func TestRunReturnsConfigurationErrors(t *testing.T) {
	t.Setenv("RHIZA_ROLE", "unknown")
	t.Setenv("RHIZA_OBJSTORE_INSECURE", "")
	if err := run(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid RHIZA_ROLE") {
		t.Fatalf("error=%v", err)
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
