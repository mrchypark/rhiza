package materializer

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/types"
)

func TestKVExpiryPruningIsBoundedIdempotentAndConvergent(t *testing.T) {
	ctx := context.Background()
	first, err := Open(filepath.Join(t.TempDir(), "first.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := Open(filepath.Join(t.TempDir(), "second.db"), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	commands := make([]types.KVCommand, 0, 260)
	for i := 0; i < 257; i++ {
		commands = append(commands, types.KVCommand{
			RequestID: fmt.Sprintf("expired-%03d", i), Operation: "put", Key: fmt.Sprintf("expired-%03d", i), Value: []byte("old"),
			ObservedAtUnixMS: 1, ExpiresAtUnixMS: 10,
		})
	}
	trigger := types.KVCommand{RequestID: "trigger", Operation: "put", Key: "live", Value: []byte("new"), ObservedAtUnixMS: 10}
	commands = append(commands, trigger, trigger, types.KVCommand{RequestID: "finish", Operation: "put", Key: "later", Value: []byte("new"), ObservedAtUnixMS: 10})

	for i, command := range commands {
		value := mustKVValue(t, command)
		for _, materializer := range []*Materializer{first, second} {
			if err := materializer.Apply(ctx, uint64(i+1), value); err != nil {
				t.Fatalf("apply %q: %v", command.RequestID, err)
			}
		}
		if i == 256 { // Reads hide expiry, but idle physical state retains all rows.
			for _, materializer := range []*Materializer{first, second} {
				if got := expiredKVRows(t, materializer); got != 257 {
					t.Fatalf("expired rows before next mutation=%d, want 257", got)
				}
				if _, found, err := materializer.KVGet(ctx, "expired-000", time.UnixMilli(10)); err != nil || found {
					t.Fatalf("expired read found=%v err=%v", found, err)
				}
			}
		}
		if i == 257 { // First trigger removes no more than the fixed 256-row batch.
			for _, materializer := range []*Materializer{first, second} {
				if got := expiredKVRows(t, materializer); got != 1 {
					t.Fatalf("expired rows after first batch=%d, want 1", got)
				}
				if _, found, err := materializer.KVGet(ctx, "expired-256", time.UnixMilli(10)); err != nil || found {
					t.Fatalf("expired read found=%v err=%v", found, err)
				}
			}
			if got, want := fmt.Sprint(expiredKVKeys(t, first)), fmt.Sprint(expiredKVKeys(t, second)); got != want {
				t.Fatalf("independent materializers retained different rows: %q != %q", got, want)
			}
		}
		if i == 258 { // Retrying the same request neither writes nor runs another prune.
			for _, materializer := range []*Materializer{first, second} {
				if got := expiredKVRows(t, materializer); got != 1 {
					t.Fatalf("expired rows after retry=%d, want 1", got)
				}
			}
			if got, want := fmt.Sprint(expiredKVKeys(t, first)), fmt.Sprint(expiredKVKeys(t, second)); got != want {
				t.Fatalf("retry diverged materializers: %q != %q", got, want)
			}
		}
	}
	for _, materializer := range []*Materializer{first, second} {
		if got := expiredKVRows(t, materializer); got != 0 {
			t.Fatalf("expired rows after next command=%d, want 0", got)
		}
	}
}

func TestKVBatchPreparedWritesRetainOrderingAndRollback(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "kv.db")
	m, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	commands := []types.KVCommand{
		{RequestID: "put", Operation: "put", Key: "k", Value: []byte("first"), ObservedAtUnixMS: 1, ExpiresAtUnixMS: 2},
		{RequestID: "cas", Operation: "cas", Key: "k", Value: []byte("second"), ObservedAtUnixMS: 2},
		{RequestID: "put", Operation: "put", Key: "k", Value: []byte("duplicate"), ObservedAtUnixMS: 2},
		{RequestID: "mismatch", Operation: "cas", Key: "k", Value: []byte("unexpected"), ObservedAtUnixMS: 2},
	}
	invalid := types.KVCommand{RequestID: "invalid", Operation: "invalid", Key: "k"}
	if err := m.Apply(ctx, 1, encodeKVTestBatch(t, append(commands, invalid))); err == nil {
		t.Fatal("invalid command did not roll back the batch")
	}
	if _, found, err := m.MutationReceipt(ctx, types.MutationKV, "cas"); err != nil || found {
		t.Fatalf("rolled-back CAS receipt found=%v err=%v", found, err)
	}
	if err := m.Apply(ctx, 1, encodeKVTestBatch(t, commands)); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	m = reopened
	got, found, err := m.KVGet(ctx, "k", time.UnixMilli(2))
	if err != nil || !found || string(got) != "second" {
		t.Fatalf("reopened key value=%q found=%v err=%v", got, found, err)
	}
	for id, applied := range map[string]bool{"put": true, "cas": true, "mismatch": false} {
		receipt, found, err := m.MutationReceipt(ctx, types.MutationKV, id)
		if err != nil || !found || receipt.Slot != 1 || receipt.Applied != applied {
			t.Fatalf("reopened receipt %q=%+v found=%v err=%v", id, receipt, found, err)
		}
	}
}

func BenchmarkKVBatchApply(b *testing.B) {
	for _, size := range []int{1, 16} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			m, err := Open(filepath.Join(b.TempDir(), "kv.db"), 1)
			if err != nil {
				b.Fatal(err)
			}
			defer m.Close()
			commands := make([]types.KVCommand, size)
			for i := range commands {
				commands[i] = types.KVCommand{Operation: "put", Key: fmt.Sprint(i), Value: []byte("value")}
			}
			b.ReportAllocs()
			for iteration := 0; b.Loop(); iteration++ {
				for i := range commands {
					commands[i].RequestID = fmt.Sprint(iteration*size + i)
				}
				if err := m.Apply(b.Context(), uint64(iteration+1), encodeKVTestBatch(b, commands)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func encodeKVTestBatch(t testing.TB, commands []types.KVCommand) []byte {
	t.Helper()
	items := make([][]byte, len(commands))
	for i, command := range commands {
		item, err := types.EncodeKVBatchItem(command)
		if err != nil {
			t.Fatal(err)
		}
		items[i] = item
	}
	return types.AssembleKVBatch(items)
}

func expiredKVKeys(t testing.TB, materializer *Materializer) []string {
	t.Helper()
	rows, err := materializer.writer.Query(`SELECT key FROM _rhiza_kv WHERE expires_at_unix_ms > 0 AND expires_at_unix_ms <= 10 ORDER BY key`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return keys
}

func expiredKVRows(t testing.TB, materializer *Materializer) int {
	t.Helper()
	var count int
	if err := materializer.writer.QueryRow(`SELECT COUNT(*) FROM _rhiza_kv WHERE expires_at_unix_ms > 0 AND expires_at_unix_ms <= 10`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
