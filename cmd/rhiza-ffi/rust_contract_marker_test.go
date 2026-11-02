//go:build cgo

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/managedbootstrap"
	"github.com/mrchypark/rhiza/pkg/operator"
)

// This marker is CI run control, not a membership/startup authority or receipt.
// A partial/unknown publication is retained and cannot authorize initial SQL.
func rustContractRunMode(path string) (bool, error) {
	const content = "managed-rust-contract-started-v1\n"
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	initial := err == nil
	if errors.Is(err, os.ErrExist) {
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return false, errors.New("invalid existing Rust run-start marker")
		}
		f, err = os.Open(path)
		if err != nil {
			return false, err
		}
		opened, statErr := f.Stat()
		if statErr != nil || !os.SameFile(info, opened) {
			_ = f.Close()
			return false, errors.New("Rust run-start marker changed")
		}
		got, readErr := io.ReadAll(io.LimitReader(f, int64(len(content)+1)))
		if readErr != nil || !bytes.Equal(got, []byte(content)) {
			_ = f.Close()
			return false, errors.New("corrupt Rust run-start marker")
		}
	} else if err == nil {
		_, err = f.WriteString(content)
	}
	if err != nil {
		if f != nil {
			_ = f.Close()
		}
		return false, err
	}
	err = errors.Join(f.Sync(), f.Close())
	if err != nil {
		return false, err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	if err := errors.Join(dir.Sync(), dir.Close()); err != nil {
		return false, err
	}
	return initial, nil
}

func rustFixtureProfile(protocol string) (string, error) {
	switch protocol {
	case "":
		return "sql", nil
	case types.ManagedApplicationProtocolSQLKV:
		return "mixed", nil
	case sqlpolicy.ManagedRequestApplicationProtocol:
		return "internal", nil
	default:
		return "", errors.New("unsupported explicit Rust fixture protocol")
	}
}

func TestRustFixtureProfileSelection(t *testing.T) {
	for _, tc := range []struct{ protocol, want string }{{"", "sql"}, {types.ManagedApplicationProtocolSQLKV, "mixed"}, {sqlpolicy.ManagedRequestApplicationProtocol, "internal"}} {
		got, err := rustFixtureProfile(tc.protocol)
		if err != nil || got != tc.want {
			t.Fatalf("profile selection %q: %q %v", tc.protocol, got, err)
		}
	}
	if _, err := rustFixtureProfile("rhiza-managed-request/v2"); err == nil {
		t.Fatal("unknown profile fell back")
	}
}

// The profile in the fixture secret alone is not proof that native code opened
// the requested materializer. Inspect the actual file after the contract closes.
func TestManagedInternalMaterializationMarker(t *testing.T) {
	if os.Getenv("RHIZA_INTERNAL_MATERIALIZATION_MARKER") != "1" {
		t.Skip("CI-only actual internal materialization check")
	}
	file, err := os.Open(operator.V1Alpha2ManagedRuntimeProfilePath)
	if err != nil {
		t.Fatal("internal runtime profile unavailable")
	}
	profile, decodeErr := operator.DecodeV1Alpha2RuntimeProfile(file)
	if err := errors.Join(decodeErr, file.Close()); err != nil || profile.ApplicationProtocol != sqlpolicy.ManagedRequestApplicationProtocol {
		t.Fatal("actual runtime profile is not internal")
	}
	if err := sqlpolicy.CheckFileForPolicy(context.Background(), filepath.Join(operator.V1Alpha2BootstrapDataDir, "data", "sqlite.db"), sqlpolicy.ManagedRequestMaterializationPolicy); err != nil {
		t.Fatal("actual internal materialization marker mismatch")
	}
	fmt.Println("RHIZA_ACTUAL_MATERIALIZATION_PROFILE=9")
}

func TestRustContractRunStartedMarker(t *testing.T) {
	if os.Getenv("RHIZA_RUST_CONTRACT_MARKER") != "1" {
		t.Skip("CI-only durable Rust initial-run selection")
	}
	profileFile, err := os.Open(operator.V1Alpha2ManagedRuntimeProfilePath)
	if err != nil {
		t.Fatal("contract profile unavailable")
	}
	profile, decodeErr := operator.DecodeV1Alpha2RuntimeProfile(profileFile)
	if err := errors.Join(decodeErr, profileFile.Close()); err != nil {
		t.Fatal("invalid contract profile")
	}
	fixtureProfile, err := rustFixtureProfile(profile.ApplicationProtocol)
	if err != nil {
		t.Fatal("invalid Rust fixture profile")
	}
	if fixtureProfile == "mixed" || fixtureProfile == "internal" {
		var plan operator.V1Alpha2BootstrapPlan
		if err := managedbootstrap.ReadGateFile(operator.V1Alpha2BootstrapHandoffPath, &plan, 65536, false); err != nil {
			t.Fatal("contract bootstrap unavailable")
		}
		_, repairErr := os.Lstat(managedbootstrap.RepairHandoffPath)
		if repairErr != nil && !errors.Is(repairErr, os.ErrNotExist) {
			t.Fatal("repair presence unknown")
		}
		producer, role, err := mixedRustRole(plan, os.Getenv("POD_UID"), repairErr == nil)
		if err != nil {
			t.Fatal("invalid original producer binding")
		}
		if role == "initial" {
			initial, err := rustContractRunMode("/var/lib/rhiza/rust-mixed-contract-started")
			if err != nil {
				t.Fatal("mixed run marker failed")
			}
			if !initial {
				role = "verify"
			}
		}
		fmt.Println("RHIZA_RUST_FIXTURE_PROFILE=" + fixtureProfile)
		fmt.Println("RHIZA_RUST_CONTRACT_MODE=" + role)
		fmt.Println("RHIZA_RUST_CONTRACT_PRODUCER_UID=" + producer)
		return
	}
	fmt.Println("RHIZA_RUST_FIXTURE_PROFILE=sql")
	if _, err := os.Lstat(managedbootstrap.RepairHandoffPath); errors.Is(err, os.ErrNotExist) {
		fmt.Println("RHIZA_RUST_CONTRACT_MODE=skip")
		return
	} else if err != nil {
		t.Fatal("repair presence unknown")
	}
	initial, err := rustContractRunMode("/var/lib/rhiza/rust-contract-started")
	if err != nil {
		t.Fatal("Rust run-start marker publication/validation failed")
	}
	if initial {
		fmt.Println("RHIZA_RUST_CONTRACT_MODE=initial")
	} else {
		fmt.Println("RHIZA_RUST_CONTRACT_MODE=restart")
	}
}

func mixedRustRole(plan operator.V1Alpha2BootstrapPlan, uid string, repair bool) (string, string, error) {
	if len(plan.Members) != 3 || uid == "" {
		return "", "", errors.New("three original members required")
	}
	members := append([]operator.V1Alpha2BootstrapMember(nil), plan.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i].Member.ID < members[j].Member.ID })
	seen := map[string]bool{}
	for _, member := range members {
		id, err := types.ManagedNodeID(member.PodUID)
		if err != nil || member.Member.ID != id || seen[member.PodUID] {
			return "", "", errors.New("invalid member")
		}
		seen[member.PodUID] = true
	}
	producer := members[0].PodUID
	if repair {
		return producer, "verify", nil
	}
	if !seen[uid] {
		return "", "", errors.New("nonoriginal Pod requires repair")
	}
	if uid != producer {
		return producer, "skip", nil
	}
	return producer, "initial", nil
}

type rustPeerPod struct {
	Metadata struct {
		UID               string  `json:"uid"`
		DeletionTimestamp *string `json:"deletionTimestamp"`
	} `json:"metadata"`
	Status struct {
		Conditions        []struct{ Type, Status string } `json:"conditions"`
		ContainerStatuses []struct {
			Name         string
			Ready        bool
			RestartCount int
		} `json:"containerStatuses"`
	} `json:"status"`
}

func rustOriginalPeersReady(plan operator.V1Alpha2BootstrapPlan, producer string, pods []rustPeerPod) bool {
	want := map[string]bool{}
	for _, member := range plan.Members {
		if member.PodUID != producer {
			want[member.PodUID] = false
		}
	}
	if len(want) != 2 {
		return false
	}
	for _, pod := range pods {
		ready, ok := want[pod.Metadata.UID]
		if !ok {
			continue
		}
		if ready || pod.Metadata.DeletionTimestamp != nil {
			return false
		}
		podReady, mainReady := false, false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == "True" {
				podReady = true
			}
		}
		for _, container := range pod.Status.ContainerStatuses {
			if container.Name == "sql" && container.Ready && container.RestartCount == 0 {
				mainReady = true
			}
		}
		if !podReady || !mainReady {
			return false
		}
		want[pod.Metadata.UID] = true
	}
	for _, ready := range want {
		if !ready {
			return false
		}
	}
	return true
}

// Spawned by Rust AFTER its real ManagedDb has opened, before its first write.
// This helper owns no application Host/WAL; it performs namespace Pod LIST only.
func TestRustMixedInitialPeersReady(t *testing.T) {
	if os.Getenv("RHIZA_RUST_WAIT_ORIGINAL_PEERS") != "1" {
		t.Skip("in-cluster Rust read-only prerequisite")
	}
	budget, err := strconv.Atoi(os.Getenv("RHIZA_RUST_PEER_BUDGET_MS"))
	if err != nil || budget < 1 || budget > 90000 {
		t.Fatal("invalid peer readiness budget")
	}
	var plan operator.V1Alpha2BootstrapPlan
	if err := managedbootstrap.ReadGateFile(operator.V1Alpha2BootstrapHandoffPath, &plan, 65536, false); err != nil {
		t.Fatal("bootstrap unavailable")
	}
	producer, role, err := mixedRustRole(plan, os.Getenv("POD_UID"), false)
	if err != nil || role != "initial" {
		t.Fatal("not original producer")
	}
	kube, err := operator.NewInCluster(os.Getenv("POD_NAMESPACE"))
	if err != nil {
		t.Fatal("read-only Kubernetes client unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(budget)*time.Millisecond)
	defer cancel()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		var pods struct {
			Items []rustPeerPod `json:"items"`
		}
		query := "/api/v1/namespaces/" + url.PathEscape(os.Getenv("POD_NAMESPACE")) + "/pods?labelSelector=" + url.QueryEscape("rhiza.mrchypark.dev/cluster-uid="+os.Getenv("RHIZA_CLUSTER_UID"))
		if err := kube.Get(ctx, query, &pods); err != nil {
			t.Fatal("exact peers API unavailable")
		}
		if rustOriginalPeersReady(plan, producer, pods.Items) {
			fmt.Println("PASS: exact original Rust peers normal Ready")
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("exact original peers not Ready")
		case <-tick.C:
		}
	}
}

func TestRustMixedOriginalRoleAndPeerBoundary(t *testing.T) {
	var plan operator.V1Alpha2BootstrapPlan
	for _, uid := range []string{"original-a", "original-b", "original-c"} {
		id, err := types.ManagedNodeID(uid)
		if err != nil {
			t.Fatal(err)
		}
		plan.Members = append(plan.Members, operator.V1Alpha2BootstrapMember{PodUID: uid})
		plan.Members[len(plan.Members)-1].Member.ID = id
	}
	producer, _, err := mixedRustRole(plan, plan.Members[0].PodUID, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range plan.Members {
		_, role, err := mixedRustRole(plan, member.PodUID, false)
		want := "skip"
		if member.PodUID == producer {
			want = "initial"
		}
		if err != nil || role != want {
			t.Fatal("original role changed", role, err)
		}
	}
	if _, _, err := mixedRustRole(plan, "replacement", false); err == nil {
		t.Fatal("empty replacement disk authorized initial")
	}
	if got, role, err := mixedRustRole(plan, "replacement", true); err != nil || got != producer || role != "verify" {
		t.Fatal("replacement role", role, err)
	}
	var pods []rustPeerPod
	for _, member := range plan.Members {
		if member.PodUID == producer {
			continue
		}
		var pod rustPeerPod
		raw, _ := json.Marshal(map[string]any{"metadata": map[string]any{"uid": member.PodUID}, "status": map[string]any{"conditions": []any{map[string]any{"type": "Ready", "status": "True"}}, "containerStatuses": []any{map[string]any{"name": "sql", "ready": true, "restartCount": 0}}}})
		if err := json.Unmarshal(raw, &pod); err != nil {
			t.Fatal(err)
		}
		pods = append(pods, pod)
	}
	if rustOriginalPeersReady(plan, producer, pods[:1]) {
		t.Fatal("late peer allowed first mutation")
	}
	if !rustOriginalPeersReady(plan, producer, pods) {
		t.Fatal("exact OTHER two ready peers rejected")
	}
	pods[0].Status.ContainerStatuses[0].RestartCount = 1
	if rustOriginalPeersReady(plan, producer, pods) {
		t.Fatal("failed peer initial restart was masked")
	}
	pods[0].Status.ContainerStatuses[0].RestartCount = 0
	pods[0].Metadata.UID = "replacement"
	if rustOriginalPeersReady(plan, producer, pods) {
		t.Fatal("replacement substituted for original peer")
	}
}

func TestRustContractRunMarkerIsExclusiveAndFailClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "started")
	initial, err := rustContractRunMode(path)
	if err != nil || !initial {
		t.Fatalf("first durable publication: %v", err)
	}
	// A crash after start, even without any ACK marker, is a restart—not a new
	// initial SQL permission. Rust must validate its retained prior ACK itself.
	initial, err = rustContractRunMode(path)
	if err != nil || initial {
		t.Fatalf("existing start was treated as initial: %v", err)
	}
	concurrent := filepath.Join(dir, "concurrent")
	var starts atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			first, err := rustContractRunMode(concurrent)
			if err != nil && first {
				t.Error("unknown publication authorized initial SQL")
			}
			if first {
				starts.Add(1)
			}
		})
	}
	wg.Wait()
	if starts.Load() != 1 {
		t.Fatal("concurrent publication authorized multiple initial runs")
	}
	if first, err := rustContractRunMode(concurrent); err != nil || first {
		t.Fatalf("durable concurrent winner missing: %v", err)
	}
	for _, name := range []string{"corrupt", "symlink", "public"} {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(dir, name)
			if name == "symlink" {
				err = os.Symlink(path, p)
			} else {
				mode := os.FileMode(0600)
				if name == "public" {
					mode = 0644
				}
				err = os.WriteFile(p, []byte("unknown"), mode)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := os.Lstat(p)
			if _, err := rustContractRunMode(p); err == nil {
				t.Fatal("unsafe existing marker accepted")
			}
			after, err := os.Lstat(p)
			if err != nil || !os.SameFile(before, after) {
				t.Fatal("unknown marker replaced")
			}
		})
	}
}
