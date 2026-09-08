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
