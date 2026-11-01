package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	objectstore "github.com/mrchypark/rhiza/internal/objstore"
	"github.com/mrchypark/rhiza/pkg/operator"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"github.com/mrchypark/rhiza/pkg/recovery"
)

func TestImmutableRepairPublication(t *testing.T) {
	dir := t.TempDir()
	identity, output := filepath.Join(dir, "identity.json"), filepath.Join(dir, "repair.json")
	if err := os.WriteFile(identity, []byte("private identity"), 0600); err != nil {
		t.Fatal(err)
	}
	value := repairGateFile{Version: 1}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := writeImmutableRepairFile(identity, output, value); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	original, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(output)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("private origin mode: %v, %v", info, err)
	}
	value.Version++
	if err := writeImmutableRepairFile(identity, output, value); err == nil {
		t.Fatal("replaced immutable origin")
	}
	after, err := os.ReadFile(output)
	if err != nil || string(after) != string(original) {
		t.Fatal("mismatch did not preserve original")
	}
	for _, name := range []string{"corrupt", "public", "symlink"} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, name)
			if name == "symlink" {
				err = os.Symlink(output, path)
			} else {
				mode := os.FileMode(0600)
				if name == "public" {
					mode = 0644
				}
				err = os.WriteFile(path, []byte("corrupt"), mode)
			}
			if err != nil {
				t.Fatal(err)
			}
			before, _ := os.Lstat(path)
			if err := writeImmutableRepairFile(identity, path, repairGateFile{Version: 1}); err == nil {
				t.Fatal("accepted unsafe existing origin")
			}
			after, err := os.Lstat(path)
			if err != nil || !os.SameFile(before, after) {
				t.Fatal("existing origin replaced")
			}
		})
	}
	entries, err := filepath.Glob(filepath.Join(dir, ".rhiza-repair-*"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary files remain: %v, %v", entries, err)
	}
}

// Actual in-memory delivery between real Core/WAL instances; no fabricated
// decisions, certificates, learner registration, or status authority.
type repairCoreTransport map[quepaxa.NodeID]*quepaxa.Core

func (t repairCoreTransport) SendRecord(ctx context.Context, id quepaxa.NodeID, r quepaxa.RecordRequest) (quepaxa.Summary, error) {
	return t[id].Record(ctx, r)
}
func (t repairCoreTransport) SendDecision(ctx context.Context, d quepaxa.Decision) error {
	for _, c := range t {
		if err := c.AcceptDecision(d); err != nil {
			return err
		}
	}
	return nil
}
func (t repairCoreTransport) ReadTip(_ context.Context, id quepaxa.NodeID) (quepaxa.Slot, error) {
	return t[id].Tip(), nil
}
func (t repairCoreTransport) StageValue(ctx context.Context, id quepaxa.NodeID, h quepaxa.ValueHash, b []byte) error {
	return t[id].StageValue(h, b)
}
func (t repairCoreTransport) FetchValue(_ context.Context, id quepaxa.NodeID, h quepaxa.ValueHash) ([]byte, error) {
	v, ok := t[id].Value(h)
	if !ok {
		return nil, errors.New("value absent")
	}
	return v, nil
}

func TestRepairControlUsesActualFixedHEADAndPreparedPrimitives(t *testing.T) {
	ctx := context.Background()
	bootstrap := quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "a"}, {ID: "b"}}}
	transport := repairCoreTransport{}
	for _, m := range bootstrap.Members {
		wal, err := qlog.Open(filepath.Join(t.TempDir(), "wal"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		core, err := quepaxa.New(quepaxa.Config{NodeID: m.ID, Cluster: bootstrap, WAL: wal, Transport: transport, EnableReconfiguration: true})
		if err != nil {
			t.Fatal(err)
		}
		transport[m.ID] = core
	}
	core := transport["a"]
	if _, err := core.BeginReconfiguration(ctx, quepaxa.Cluster{ConfigID: 2, Members: bootstrap.Members[:1]}); err != nil {
		t.Fatal(err)
	}
	if err := core.FinishReconfiguration(ctx); err != nil {
		t.Fatal(err)
	}
	bucket, err := objectstore.NewBucket(objectstore.Config{Provider: objectstore.ProviderFilesystem, FilesystemDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bucket.Close() })
	archive := recovery.NewManager(bucket, "source", 1)
	t.Cleanup(archive.Close)
	if err := archive.SyncThrough(ctx, core, core.Tip()); err != nil {
		t.Fatal(err)
	}
	view, err := recovery.OpenReadOnlyView(ctx, bucket, "source", 1, bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	terminal, ok := core.CertifiedValue(core.Tip())
	if !ok {
		t.Fatal("certified REMOVE missing")
	}
	capture := operator.V1Alpha2RepairHandoff{PlanDigest: strings.Repeat("a", 64), SourcePrefix: "source", Head: view.CapturedHead(), HeadHash: fmt.Sprintf("%x", view.HeadHash()), RemoveTerminalSlot: uint64(terminal.Slot), RemoveTerminalHash: fmt.Sprintf("%x", terminal.Hash), Survivors: core.CurrentCluster()}
	readonly, err := objectstore.BindReadOnlyNamespace(bucket, "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyRepairControl(ctx, readonly, bootstrap, view, capture); err != nil {
		t.Fatal(err)
	}
	bad := capture
	bad.RemoveTerminalHash = strings.Repeat("f", 64)
	if err := verifyRepairControl(ctx, readonly, bootstrap, view, bad); err == nil {
		t.Fatal("unrelated terminal accepted")
	}
	identity := recovery.PreparedStartupIdentity{ClusterUID: "uid", Generation: 0, DeploymentUID: "deployment", PodUID: "replacement", NodeID: "new", SourceBinding: "source-binding", ConfigDigest: "member-binding"}
	handoffDir := filepath.Join(t.TempDir(), "handoff")
	if err := recovery.PublishPreparedStartup(ctx, view, handoffDir, identity); err != nil {
		t.Fatal(err)
	}
	prepared, err := recovery.LoadPreparedStartup(ctx, readonly, "source", 1, bootstrap, handoffDir, filepath.Join(t.TempDir(), "verified"), identity)
	if err != nil || prepared.Identity() != identity || prepared.View().HeadHash() != view.HeadHash() {
		t.Fatalf("actual prepared handoff load: %v", err)
	}
}
