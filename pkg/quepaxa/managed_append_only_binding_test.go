package quepaxa

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

func TestManagedAppendOnlyAcceptsFreshRegisteredEmptyRoster(t *testing.T) {
	config, wal := managedAppendOnlyTestConfig(t, managedRequestAppendOnlyApplicationProtocol, func(_ context.Context, _ ManagedCheckpointCandidate) error { return nil })
	defer wal.Close()
	config.Cluster.Members[0].WALIdentity = ""
	wantIdentity := hex.EncodeToString(config.ManagedAppendOnly.Binding.WALIdentity[:])

	core, err := New(config)
	if err != nil {
		t.Fatalf("fresh registered WAL with common empty roster identity was rejected: %v", err)
	}
	if got := core.WALIdentity(); got != wantIdentity {
		t.Fatalf("Core WAL identity=%q, want registered binding %q", got, wantIdentity)
	}
	if config.Cluster.Members[0].WALIdentity != "" {
		t.Fatal("Core construction rewrote the original common roster")
	}
}

func TestManagedAppendOnlyStillRejectsWrongOrMissingWALBinding(t *testing.T) {
	t.Run("nonempty wrong roster nonce", func(t *testing.T) {
		config, wal := managedAppendOnlyTestConfig(t, managedRequestAppendOnlyApplicationProtocol, func(_ context.Context, _ ManagedCheckpointCandidate) error { return nil })
		defer wal.Close()
		config.Cluster.Members[0].WALIdentity = hex.EncodeToString(bytes.Repeat([]byte{0x7b}, 32))
		if _, err := New(config); err == nil {
			t.Fatal("accepted nonempty roster WAL identity that differs from the bound WAL")
		}
	})

	t.Run("missing binding nonce", func(t *testing.T) {
		config, wal := managedAppendOnlyTestConfig(t, managedRequestAppendOnlyApplicationProtocol, func(_ context.Context, _ ManagedCheckpointCandidate) error { return nil })
		defer wal.Close()
		config.Cluster.Members[0].WALIdentity = ""
		config.ManagedAppendOnly.Binding.WALIdentity = [32]byte{}
		if _, err := New(config); err == nil {
			t.Fatal("accepted append-only profile without a fixed WAL binding")
		}
	})

	t.Run("unregistered WAL", func(t *testing.T) {
		config, wal := managedAppendOnlyTestConfig(t, managedRequestAppendOnlyApplicationProtocol, func(_ context.Context, _ ManagedCheckpointCandidate) error { return nil })
		defer wal.Close()
		unregistered, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer unregistered.Close()
		config.WAL = unregistered
		config.Cluster.Members[0].WALIdentity = ""
		if _, err := New(config); err == nil {
			t.Fatal("accepted an unregistered old WAL for a bound append-only profile")
		}
	})

	t.Run("different registered old WAL", func(t *testing.T) {
		config, wal := managedAppendOnlyTestConfig(t, managedRequestAppendOnlyApplicationProtocol, func(_ context.Context, _ ManagedCheckpointCandidate) error { return nil })
		defer wal.Close()
		oldWAL, err := qlog.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer oldWAL.Close()
		if err := oldWAL.BindIdentity(bytes.Repeat([]byte{0x33}, 32)); err != nil {
			t.Fatal(err)
		}
		config.WAL = oldWAL
		config.Cluster.Members[0].WALIdentity = ""
		if _, err := New(config); err == nil {
			t.Fatal("accepted a different registered old WAL for the fixed append-only binding")
		}
	})
}
