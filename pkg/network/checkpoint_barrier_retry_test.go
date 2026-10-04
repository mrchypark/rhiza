package network

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestCheckpointBarrierRetriesRealPreAdmissionWithSamePayload(t *testing.T) {
	core := mustCore(t, "n1", []quepaxa.Member{{ID: "n1"}}, nil, nil)
	material, err := materializer.Open(t.TempDir()+"/db.sqlite", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer material.Close()
	server := NewServer(core, material, "cluster", true, nil)
	defer server.Close()
	for range cap(server.localCap) {
		server.localCap <- struct{}{}
	}
	barrier := types.EncodeReadBarrier([types.ReadBarrierNonceSize]byte{1, 2, 3})
	var calls int
	slot, err := retryCheckpointBarrier(context.Background(), barrier, func(ctx context.Context, value []byte) (quepaxa.Slot, error) {
		calls++
		if !bytes.Equal(value, barrier) {
			t.Fatal("checkpoint barrier payload changed between attempts")
		}
		gotSlot, gotErr := server.ProposeControl(ctx, value)
		if calls == 1 {
			var admissionErr proposalAdmissionOverload
			if !errors.As(gotErr, &admissionErr) {
				t.Fatalf("first real guard error=%v, want typed pre-admission refusal", gotErr)
			}
			<-server.localCap
		}
		return gotSlot, gotErr
	})
	if err != nil || slot == 0 || calls != 2 {
		t.Fatalf("checkpoint barrier slot=%d err=%v calls=%d, want committed slot and two attempts", slot, err, calls)
	}
}

func TestCheckpointBarrierRetryTerminalOutcomes(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		slot quepaxa.Slot
	}{
		{"plain overload", ErrOverloaded, 0},
		{"wrapped typed overload", fmt.Errorf("wrapped: %w", proposalAdmissionOverload{}), 0},
		{"joined typed and commit unknown", errors.Join(proposalAdmissionOverload{}, ErrCommitUnknown), 0},
		{"commit unknown", ErrCommitUnknown, 0},
		{"fencing", errors.New("publisher fenced"), 0},
		{"storage", errors.New("storage unavailable"), 0},
		{"typed error with accepted slot", proposalAdmissionOverload{}, 9},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			slot, err := retryCheckpointBarrier(context.Background(), []byte("barrier"), func(context.Context, []byte) (quepaxa.Slot, error) {
				calls++
				return tt.slot, tt.err
			})
			if calls != 1 || slot != tt.slot || err != tt.err {
				t.Fatalf("calls=%d slot=%d err=%v, want one terminal call, slot=%d err=%v", calls, slot, err, tt.slot, tt.err)
			}
		})
	}

	t.Run("canceled before first call", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		_, err := retryCheckpointBarrier(ctx, []byte("barrier"), func(context.Context, []byte) (quepaxa.Slot, error) {
			calls++
			return 0, nil
		})
		if !errors.Is(err, context.Canceled) || calls != 0 {
			t.Fatalf("err=%v calls=%d, want cancellation before any call", err, calls)
		}
	})

	t.Run("cancellation during retry delay", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		calls := 0
		_, err := retryCheckpointBarrier(ctx, []byte("barrier"), func(context.Context, []byte) (quepaxa.Slot, error) {
			calls++
			cancel()
			return 0, proposalAdmissionOverload{}
		})
		if !errors.Is(err, context.Canceled) || calls != 1 {
			t.Fatalf("err=%v calls=%d, want cancellation after one call", err, calls)
		}
	})

	t.Run("fifty total attempts", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		calls := 0
		_, err := retryCheckpointBarrier(ctx, []byte("barrier"), func(context.Context, []byte) (quepaxa.Slot, error) {
			calls++
			return 0, proposalAdmissionOverload{}
		})
		var denied proposalAdmissionOverload
		if !errors.As(err, &denied) || calls != 50 {
			t.Fatalf("err=%v calls=%d, want original typed refusal after 50 total calls", err, calls)
		}
	})
}
