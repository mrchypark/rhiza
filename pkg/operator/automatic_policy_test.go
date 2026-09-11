package operator

import (
	"testing"
	"time"
)

var (
	tBase      = time.Date(2026, 9, 11, 12, 0, 0, 0, time.UTC)
	goodPolicy = AutoPolicy{
		FailureGraceSeconds: 300,
		CooldownSeconds:     60,
		MaxRecoveriesPerDay: 3,
		AllowDataLoss:       false,
	}
	healthyObs = AutoObservation{
		Known:          true,
		Voters:         3,
		Reachable:      3,
		QuorumVerified: true,
		StoreAvailable: true,
		Durability:     "before-ack",
	}
)

func TestAutoDecisionEnum(t *testing.T) {
	want := map[AutoDecision]string{
		AutoHealthy:  "Healthy",
		AutoDegraded: "Degraded",
		AutoSuspect:  "Suspect",
		AutoBlocked:  "Blocked",
		AutoRecover:  "Recover",
	}
	for d, s := range want {
		if string(d) != s {
			t.Errorf("%v = %q, want %q", d, string(d), s)
		}
	}
}

func TestBlockedZeroGraceSeconds(t *testing.T) {
	p := goodPolicy
	p.FailureGraceSeconds = 0
	d, _ := DecideAutoRecovery(p, healthyObs, tBase, tBase, nil)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestBlockedZeroCooldownSeconds(t *testing.T) {
	p := goodPolicy
	p.CooldownSeconds = 0
	d, _ := DecideAutoRecovery(p, healthyObs, tBase, tBase, nil)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestBlockedZeroMaxRecoveriesPerDay(t *testing.T) {
	p := goodPolicy
	p.MaxRecoveriesPerDay = 0
	d, _ := DecideAutoRecovery(p, healthyObs, tBase, tBase, nil)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestBlockedNegativePolicyFields(t *testing.T) {
	p := goodPolicy
	p.FailureGraceSeconds = -1
	d, _ := DecideAutoRecovery(p, healthyObs, tBase, tBase, nil)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestBlockedUnknownObservation(t *testing.T) {
	obs := AutoObservation{Known: false}
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, tBase, nil)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestBlockedStoreUnavailable(t *testing.T) {
	obs := healthyObs
	obs.StoreAvailable = false
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, tBase, nil)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestBlockedInvalidDurability(t *testing.T) {
	obs := healthyObs
	obs.Durability = "unknown-mode"
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, tBase, nil)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestHealthyFullQuorum(t *testing.T) {
	d, reason := DecideAutoRecovery(goodPolicy, healthyObs, tBase, tBase, nil)
	if d != AutoHealthy {
		t.Fatalf("got %v, want Healthy: %s", d, reason)
	}
}

func TestDegradedQuorumButUnreachable(t *testing.T) {
	obs := healthyObs
	obs.Reachable = 2
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, tBase, nil)
	if d != AutoDegraded {
		t.Fatalf("got %v, want Degraded", d)
	}
}

func TestSuspectNoQuorumNoTimestamp(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, time.Time{}, nil)
	if d != AutoSuspect {
		t.Fatalf("got %v, want Suspect", d)
	}
}

func TestSuspectWithinGracePeriod(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	suspected := tBase.Add(-200 * time.Second) // within 300s grace
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, suspected, nil)
	if d != AutoSuspect {
		t.Fatalf("got %v, want Suspect", d)
	}
}

func TestBlockedAsyncWithoutAllowDataLoss(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	obs.Durability = "async"
	p := goodPolicy
	p.AllowDataLoss = false
	suspected := tBase.Add(-600 * time.Second) // past grace
	d, _ := DecideAutoRecovery(p, obs, tBase, suspected, nil)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestRecoverAsyncWithAllowDataLoss(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	obs.Durability = "async"
	p := goodPolicy
	p.AllowDataLoss = true
	suspected := tBase.Add(-600 * time.Second) // past grace
	d, _ := DecideAutoRecovery(p, obs, tBase, suspected, nil)
	if d != AutoRecover {
		t.Fatalf("got %v, want Recover", d)
	}
}

func TestRecoverBeforeAckAfterGrace(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	suspected := tBase.Add(-600 * time.Second) // past grace
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, suspected, nil)
	if d != AutoRecover {
		t.Fatalf("got %v, want Recover", d)
	}
}

func TestBlockedCooldownNotElapsed(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	suspected := tBase.Add(-600 * time.Second)
	completed := []time.Time{tBase.Add(-30 * time.Second)} // 30s ago, cooldown is 60s
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, suspected, completed)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestRecoverCooldownElapsed(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	suspected := tBase.Add(-600 * time.Second)
	completed := []time.Time{tBase.Add(-90 * time.Second)} // 90s ago, cooldown is 60s
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, suspected, completed)
	if d != AutoRecover {
		t.Fatalf("got %v, want Recover", d)
	}
}

func TestBlockedDailyLimitReached(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	suspected := tBase.Add(-600 * time.Second)
	completed := []time.Time{
		tBase.Add(-1 * time.Hour),
		tBase.Add(-2 * time.Hour),
		tBase.Add(-3 * time.Hour),
	}
	// 3 recoveries in 24h, MaxRecoveriesPerDay=3
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, suspected, completed)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestRecoverUnderDailyLimit(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	suspected := tBase.Add(-600 * time.Second)
	completed := []time.Time{
		tBase.Add(-1 * time.Hour),
		tBase.Add(-2 * time.Hour),
	}
	// 2 recoveries in 24h, MaxRecoveriesPerDay=3
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, suspected, completed)
	if d != AutoRecover {
		t.Fatalf("got %v, want Recover", d)
	}
}

func TestCompletedOutside24hWindowIgnored(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	suspected := tBase.Add(-600 * time.Second)
	completed := []time.Time{
		tBase.Add(-25 * time.Hour), // outside window
		tBase.Add(-26 * time.Hour),
	}
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, suspected, completed)
	if d != AutoRecover {
		t.Fatalf("got %v, want Recover", d)
	}
}

func TestBlockedExactlyAtDailyLimit(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	suspected := tBase.Add(-600 * time.Second)
	completed := []time.Time{
		tBase.Add(-1 * time.Hour),
	}
	p := goodPolicy
	p.MaxRecoveriesPerDay = 1
	d, _ := DecideAutoRecovery(p, obs, tBase, suspected, completed)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked", d)
	}
}

func TestHealthyBeforeAckWithExtraReachable(t *testing.T) {
	obs := healthyObs
	obs.Reachable = 5 // more than Voters=3, still healthy
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, tBase, nil)
	if d != AutoHealthy {
		t.Fatalf("got %v, want Healthy", d)
	}
}

func TestDegradedZeroReachable(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = true
	obs.Reachable = 0
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, tBase, nil)
	if d != AutoDegraded {
		t.Fatalf("got %v, want Degraded", d)
	}
}

func TestSuspectZeroVotersNoQuorum(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	obs.Voters = 0
	obs.Reachable = 0
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, time.Time{}, nil)
	if d != AutoSuspect {
		t.Fatalf("got %v, want Suspect", d)
	}
}

func TestBlockedAsyncDurabilityNoLossWithRecovery(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	obs.Durability = "async"
	p := goodPolicy
	p.AllowDataLoss = false
	suspected := tBase.Add(-600 * time.Second)
	d, reason := DecideAutoRecovery(p, obs, tBase, suspected, nil)
	if d != AutoBlocked {
		t.Fatalf("got %v, want Blocked: %s", d, reason)
	}
}

func TestEmptyCompletedSliceAllowsRecovery(t *testing.T) {
	obs := healthyObs
	obs.QuorumVerified = false
	suspected := tBase.Add(-600 * time.Second)
	d, _ := DecideAutoRecovery(goodPolicy, obs, tBase, suspected, []time.Time{})
	if d != AutoRecover {
		t.Fatalf("got %v, want Recover", d)
	}
}
