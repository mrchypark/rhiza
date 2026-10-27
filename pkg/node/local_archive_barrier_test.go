package node

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/qlog"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLocalBeforeAckBarrierUsesBackupAfterDurableDecision(t *testing.T) {
	for _, callbackErr := range []error{nil, errors.New("injected Local backup failure")} {
		name := "success"
		if callbackErr != nil {
			name = "callback error propagates"
		}
		t.Run(name, func(t *testing.T) {
			walDir := filepath.Join(t.TempDir(), "qlog")
			wal, err := qlog.Open(walDir)
			if err != nil {
				t.Fatal(err)
			}
			core, err := quepaxa.New(quepaxa.Config{
				NodeID:  "local",
				Cluster: quepaxa.Cluster{ConfigID: 1, Members: []quepaxa.Member{{ID: "local"}}},
				WAL:     wal,
			})
			if err != nil {
				_ = wal.Close()
				t.Fatal(err)
			}
			material, err := materializer.Open(filepath.Join(t.TempDir(), "sqlite.db"), 1)
			if err != nil {
				_ = wal.Close()
				t.Fatal(err)
			}
			server := network.NewServer(core, material, "cluster", true, nil)
			t.Cleanup(func() {
				server.Close()
				_ = material.Close()
				_ = wal.Close()
			})

			var callbackCalled bool
			var callbackSlot quepaxa.Slot
			backupSync := func(_ context.Context) error {
				callbackCalled = true
				callbackSlot = core.Tip()
				if callbackSlot == 0 {
					return fmt.Errorf("Local backup callback ran before a decision was committed")
				}
				readOnly, err := qlog.OpenReadOnly(walDir)
				if err != nil {
					return fmt.Errorf("inspect WAL from Local backup callback: %w", err)
				}
				defer readOnly.Close()
				entries, err := readOnly.Read()
				if err != nil {
					return fmt.Errorf("read WAL from Local backup callback: %w", err)
				}
				for _, entry := range entries {
					if entry.Type == qlog.EntryDecide && entry.Slot == uint64(callbackSlot) {
						return callbackErr
					}
				}
				return fmt.Errorf("Local backup callback ran before durable decision slot %d was present", callbackSlot)
			}
			if err := installArchiveDurabilityBarriers(server, core, nil, types.ObjectStoreDurabilityBeforeAck, false, false, true, backupSync); err != nil {
				t.Fatalf("install Local backup barrier without HA archive: %v", err)
			}
			_, err = server.Execute(context.Background(), network.ExecuteRequest{
				RequestID: "local-before-ack-create",
				SQL:       "CREATE TABLE local_barrier_test (id INTEGER PRIMARY KEY)",
			})
			if !callbackCalled {
				t.Fatal("Local backup callback was not invoked")
			}
			if callbackErr == nil && err != nil {
				t.Fatalf("Local before-ack write: %v", err)
			}
			if callbackErr != nil && !errors.Is(err, callbackErr) {
				t.Fatalf("write error=%v, want Local backup cause %v", err, callbackErr)
			}
		})
	}
}

func TestArchiveDurabilityBarrierStillRequiresHAArchive(t *testing.T) {
	for _, test := range []struct {
		name       string
		durability types.ObjectStoreDurability
		supervised bool
		local      bool
	}{
		{name: "supervised async", durability: types.ObjectStoreDurabilityAsync, supervised: true},
		{name: "ordinary before ack", durability: types.ObjectStoreDurabilityBeforeAck},
		{name: "Local does not bypass supervised archive", durability: types.ObjectStoreDurabilityBeforeAck, supervised: true, local: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := installArchiveDurabilityBarriers(nil, nil, nil, test.durability, false, test.supervised, test.local, func(context.Context) error { return nil }); err == nil {
				t.Fatal("missing HA archive was accepted")
			}
		})
	}
}
