//go:build rhiza_local_testhooks

package rhiza_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
)

// TestQuorumMembershipQUICDiagnosticNoHold traces the original membership
// workload without changing its timing. The forced-hold diagnostic remains a
// separate test because it deliberately delays the learner handler.
func TestQuorumMembershipQUICDiagnosticNoHold(t *testing.T) {
	restore := localtesthooks.Set(func(event string) {
		if !strings.HasPrefix(event, "test:membership:finish-start:") &&
			!strings.HasPrefix(event, "network:terminal-decision:") &&
			!strings.HasPrefix(event, "network:learned:") &&
			!strings.HasPrefix(event, "network:learned-control:") {
			return
		}
		t.Logf("utc=%s %s", time.Now().UTC().Format(time.RFC3339Nano), event)
	})
	defer restore()

	TestQuorumMembershipQUIC(t)
}

// This reruns the real membership scenario with a short, deterministic hold
// after the promoted learner has received the terminal Learn request. The
// phase trace separates a network-delivery stall from a handler-stage stall.
func TestQuorumMembershipQUICDiagnostic(t *testing.T) {
	var mu sync.Mutex
	var events []string
	forcedHoldVerified := false
	var terminalReceive string
	restore := localtesthooks.Set(func(event string) {
		if !strings.HasPrefix(event, "network:terminal-decision:") && !strings.HasPrefix(event, "network:learned:") && !strings.HasPrefix(event, "network:learned-control:") {
			return
		}
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
		if !strings.HasPrefix(event, "network:learned-control:phase=received:node=v3:sender=v0:slot=") {
			return
		}
		mu.Lock()
		terminalReceive = event
		mu.Unlock()
		fields := strings.Split(event, ":")
		if len(fields) < 6 {
			return
		}
		slot := strings.TrimPrefix(fields[5], "slot=")
		dispatch := fmt.Sprintf("network:terminal-decision:slot=%s:peer=v3:old=2:next=3:phase=dispatch", slot)
		resultPrefix := fmt.Sprintf("network:terminal-decision:slot=%s:peer=v3:old=2:next=3:phase=result:", slot)
		mu.Lock()
		hasDispatch, hasResult := false, false
		for _, got := range events {
			hasDispatch = hasDispatch || got == dispatch
			hasResult = hasResult || strings.HasPrefix(got, resultPrefix)
		}
		mu.Unlock()
		if hasDispatch && !hasResult {
			timer := time.NewTimer(100 * time.Millisecond)
			<-timer.C
			mu.Lock()
			for _, got := range events {
				hasResult = hasResult || strings.HasPrefix(got, resultPrefix)
			}
			forcedHoldVerified = !hasResult
			mu.Unlock()
		}
	})
	defer restore()

	TestQuorumMembershipQUIC(t)

	mu.Lock()
	trace := append([]string(nil), events...)
	verified, receive := forcedHoldVerified, terminalReceive
	mu.Unlock()
	for _, event := range trace {
		t.Log(event)
	}
	if receive == "" {
		t.Fatal("promoted learner never received the terminal Learn request")
	}
	if !verified {
		t.Fatal("terminal Learn request was not observed waiting at the forced learner-handler hold")
	}
	fields := strings.Split(receive, ":")
	if len(fields) < 6 {
		t.Fatalf("unexpected terminal receive event %q", receive)
	}
	slot := strings.TrimPrefix(fields[5], "slot=")
	want := []string{
		fmt.Sprintf("network:terminal-decision:slot=%s:peer=v3:old=2:next=3:phase=dispatch", slot),
		receive,
		fmt.Sprintf("network:learned-control:phase=catchup-start:node=v3:sender=v0:slot=%s", slot),
		fmt.Sprintf("network:learned-control:phase=catchup-complete:node=v3:sender=v0:slot=%s", slot),
		fmt.Sprintf("network:learned:phase=accept-complete:node=v3:sender=v0:slot=%s", slot),
		fmt.Sprintf("network:learned:phase=apply-complete:node=v3:sender=v0:slot=%s", slot),
		fmt.Sprintf("network:learned:phase=response-written:node=v3:sender=v0:slot=%s", slot),
		fmt.Sprintf("network:terminal-decision:slot=%s:peer=v3:old=2:next=3:phase=result:error=\"\"", slot),
	}
	position := -1
	for _, expected := range want {
		found := -1
		for i := position + 1; i < len(trace); i++ {
			if trace[i] == expected {
				found = i
				break
			}
		}
		if found < 0 {
			t.Fatalf("terminal RPC phase %q missing or out of order in trace", expected)
		}
		position = found
	}
}
