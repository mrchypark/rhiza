package operator

import (
	"fmt"
	"time"
)

// AutoDecision is a string enum for automatic recovery decisions.
type AutoDecision string

const (
	AutoHealthy  AutoDecision = "Healthy"
	AutoDegraded AutoDecision = "Degraded"
	AutoSuspect  AutoDecision = "Suspect"
	AutoBlocked  AutoDecision = "Blocked"
	AutoRecover  AutoDecision = "Recover"
)

// AutoPolicy configures automatic recovery thresholds.
type AutoPolicy struct {
	FailureGraceSeconds int64 `json:"failureGraceSeconds"`
	CooldownSeconds     int64 `json:"cooldownSeconds"`
	MaxRecoveriesPerDay int   `json:"maxRecoveriesPerDay"`
	AllowDataLoss       bool  `json:"allowDataLoss"`
}

func (p AutoPolicy) validate() error {
	if p.FailureGraceSeconds < 1 || p.FailureGraceSeconds > 86400 {
		return fmt.Errorf("policy failureGraceSeconds must be between 1 and 86400")
	}
	if p.CooldownSeconds < 1 || p.CooldownSeconds > 604800 {
		return fmt.Errorf("policy cooldownSeconds must be between 1 and 604800")
	}
	if p.MaxRecoveriesPerDay < 1 || p.MaxRecoveriesPerDay > 100 {
		return fmt.Errorf("policy maxRecoveriesPerDay must be between 1 and 100")
	}
	return nil
}

func validateAutomaticPolicyForNewIncident(state *autoControl, automatic bool, policy *AutoPolicy) error {
	if !automatic || state != nil && state.Intent != nil {
		return nil
	}
	if policy == nil {
		return fmt.Errorf("automatic recovery requires an explicit policy")
	}
	return policy.validate()
}

// AutoObservation describes the current observable cluster state.
type AutoObservation struct {
	Known          bool
	Voters         int
	Reachable      int
	QuorumVerified bool
	StoreAvailable bool
	Durability     string
}

// DecideAutoRecovery is a pure function that decides whether automatic
// recovery should proceed given policy, observation, and time context.
// completed lists UTC timestamps of recoveries completed within the
// sliding 24-hour window. suspectedSince is the persisted timestamp of
// when the node first became suspect (zero value means unknown).
func DecideAutoRecovery(
	policy AutoPolicy,
	obs AutoObservation,
	now time.Time,
	suspectedSince time.Time,
	completed []time.Time,
) (AutoDecision, string) {
	if err := policy.validate(); err != nil {
		return AutoBlocked, err.Error()
	}

	// Unknown observation — cannot decide.
	if !obs.Known {
		return AutoBlocked, "observation unknown"
	}

	// Store unavailable or invalid durability mode — blocked.
	if !obs.StoreAvailable {
		return AutoBlocked, "object store unavailable"
	}
	if obs.Durability != "async" && obs.Durability != "before-ack" {
		return AutoBlocked, "invalid durability mode: must be async or before-ack"
	}

	// Full quorum with all voters reachable — healthy.
	if obs.QuorumVerified && obs.Reachable >= obs.Voters && obs.Voters > 0 {
		return AutoHealthy, "quorum verified and all voters reachable"
	}

	// Quorum verified but some voters unreachable — degraded.
	if obs.QuorumVerified {
		return AutoDegraded, "quorum verified but some voters unreachable"
	}

	// No verified quorum — require persisted suspect timestamp.
	if suspectedSince.IsZero() {
		return AutoSuspect, "no verified quorum and suspect timestamp not persisted"
	}

	// Not yet past the grace period — still suspect.
	elapsed := now.Sub(suspectedSince)
	if elapsed < time.Duration(policy.FailureGraceSeconds)*time.Second {
		return AutoSuspect, "within failure grace period"
	}

	// Past grace but async source without AllowDataLoss — blocked.
	if obs.Durability == "async" && !policy.AllowDataLoss {
		return AutoBlocked, "async source requires allowDataLoss for recovery"
	}

	// Sliding 24-hour cooldown: count completions in the last 24h.
	cutoff := now.Add(-24 * time.Hour)
	recoveriesToday := 0
	for _, t := range completed {
		if t.After(cutoff) {
			recoveriesToday++
		}
	}
	if recoveriesToday >= policy.MaxRecoveriesPerDay {
		return AutoBlocked, fmt.Sprintf("recovery limit %d/%d per 24h reached", recoveriesToday, policy.MaxRecoveriesPerDay)
	}

	// Rate limit: must wait CooldownSeconds since last completion.
	var lastCompletion time.Time
	for _, t := range completed {
		if t.After(lastCompletion) {
			lastCompletion = t
		}
	}
	if !lastCompletion.IsZero() && now.Sub(lastCompletion) < time.Duration(policy.CooldownSeconds)*time.Second {
		return AutoBlocked, "cooldown period not elapsed"
	}

	return AutoRecover, "recovery authorized"
}
