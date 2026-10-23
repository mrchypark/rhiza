package cri

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/pkg/fencing"
)

func TestPassiveObservationRequiresBothPinnedProcessesAndDetachedCgroups(t *testing.T) {
	binding := fencing.RuntimeBinding{AuthorityID: "authority", NamespaceUID: "namespace", BindingUID: "binding", StatefulSetUID: "sts",
		SourceClusterID: "source", NodeID: "node", PodUID: "pod", RunID: strings.Repeat("a", 32), NodeName: "node-name", NodeUID: "node-uid",
		MachineID: "machine", BootID: "boot", RuntimeID: "runtime", SandboxID: "sandbox", ContainerID: "containerd://container", ImageID: "image"}
	scope := CgroupScope{Path: "pod/db", Dev: 1, Ino: 2, PID: 12, StartTime: 13,
		RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ProfileID: "qualified",
		Passive: &PassiveScope{Version: 1, SandboxPID: 14, SandboxStart: 15, SandboxPath: "pod/pause", SandboxDev: 1,
			SandboxIno: 3, SandboxPIDNSDev: 4, SandboxPIDNSIno: 5, TargetPIDNSDev: 4, TargetPIDNSIno: 6,
			SandboxImage: "registry.example/pause:3", SandboxExecutableSHA256: strings.Repeat("a", 64), SandboxArgs: []string{"/pause"},
			RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ProfileID: "qualified"}}
	data, err := json.Marshal(scope)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	valid := PassiveObservation{Version: 1, RuntimeHash: fencing.RuntimeScopeBindingDigest(binding), ScopeDigest: hex.EncodeToString(digest[:]),
		TargetPIDExited: true, TargetCgroupGone: true, SandboxPIDExited: true, SandboxCgroupGone: true, ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := ValidatePassiveObservation(binding, scope, valid); err != nil {
		t.Fatalf("complete exact observation rejected: %v", err)
	}
	for name, mutate := range map[string]func(*PassiveObservation){
		"target still alive":          func(v *PassiveObservation) { v.TargetPIDExited = false },
		"target cgroup not detached":  func(v *PassiveObservation) { v.TargetCgroupGone = false },
		"sandbox still alive":         func(v *PassiveObservation) { v.SandboxPIDExited = false },
		"sandbox cgroup not detached": func(v *PassiveObservation) { v.SandboxCgroupGone = false },
		"scope mismatch":              func(v *PassiveObservation) { v.ScopeDigest = strings.Repeat("0", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			got := valid
			mutate(&got)
			if ValidatePassiveObservation(binding, scope, got) == nil {
				t.Fatal("incomplete or mismatched passive observation accepted")
			}
		})
	}
	request := fencing.StopRequest{OperationID: "stop", RequestHash: strings.Repeat("b", 64), Identity: fencing.Identity{SourceClusterID: binding.SourceClusterID, NodeID: binding.NodeID, PodUID: binding.PodUID, RunID: binding.RunID}}
	receipt, err := bindPassiveReceipt(binding, request, scope, valid)
	if err != nil {
		t.Fatalf("exact fresh request did not bind passive evidence: %v", err)
	}
	var decoded PassiveReceipt
	if err := json.Unmarshal(receipt, &decoded); err != nil || decoded.Mode != "passive-kernel-v1" || decoded.Request != request || decoded.Runtime != binding {
		t.Fatalf("passive receipt has wrong mode or tuple: %#v err=%v", decoded, err)
	}
	request.OperationID = "different"
	if _, err := bindPassiveReceipt(binding, request, scope, valid); err == nil {
		t.Fatal("passive observation was accepted for a different request")
	}
}

func TestPassiveSandboxProfileRequiresApprovedExecutableAndExactArgv(t *testing.T) {
	approved := strings.Repeat("a", 64)
	if !validPassiveSandboxProfile(approved, []string{"/pause"}, []string{"/pause"}) {
		t.Fatal("exact approved executable profile was rejected")
	}
	for name, profile := range map[string]struct {
		hash string
		args []string
	}{
		"different executable": {hash: strings.Repeat("b", 64), args: []string{"/pause"}},
		"invalid digest":       {hash: "not-a-sha256", args: []string{"/pause"}},
		"different argv":       {hash: approved, args: []string{"/pause", "--unexpected"}},
		"empty argv":           {hash: approved, args: nil},
	} {
		t.Run(name, func(t *testing.T) {
			if validPassiveSandboxProfile(profile.hash, []string{"/pause"}, profile.args) {
				t.Fatal("unapproved executable or argv was accepted")
			}
		})
	}
}
