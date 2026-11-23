//go:build !rhiza_phase_diagnostics

package quepaxa

import "time"

const phaseDiagnosticsEnabled = false

func observePhase(Step, time.Duration, error) {}
func observeDecision(Step)                    {}
