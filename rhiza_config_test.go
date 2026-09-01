package rhiza

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestOpenHedgeDelayContract(t *testing.T) {
	for name, test := range map[string]struct {
		delay *time.Duration
		want  time.Duration
	}{
		"default": {want: 100 * time.Millisecond},
		"eager":   {delay: ptrDuration(0), want: 0},
		"custom":  {delay: ptrDuration(17 * time.Millisecond), want: 17 * time.Millisecond},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := configuredHedgeDelay(test.delay)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("hedge delay = %s, want %s", got, test.want)
			}
		})
	}
	negative := -time.Millisecond
	_, err := Open(context.Background(), Config{NodeID: "n1", DataDir: t.TempDir(), HedgeDelay: &negative})
	if err == nil || !strings.Contains(err.Error(), "hedge delay") {
		t.Fatalf("negative hedge delay error = %v", err)
	}
}

func ptrDuration(value time.Duration) *time.Duration { return &value }
