package quepaxa

// RuntimeProgress is one Core-local observation of its certified and highest
// known recorder prefix. It does not turn recorder state into a decision.
type RuntimeProgress struct {
	CertifiedTip            Slot
	RecorderTip             Slot
	RecorderHash            ValueHash
	RecorderHashPresent     bool
	RecorderHashIsCertified bool
}

// RuntimeProgressSnapshot reads both tips under the same Core lock so callers
// can compare this Core's progress around observations owned by other layers.
func (c *Core) RuntimeProgressSnapshot() RuntimeProgress {
	c.mu.RLock()
	defer c.mu.RUnlock()
	progress := RuntimeProgress{CertifiedTip: c.tip, RecorderTip: c.highestKnownSlotLocked()}
	if progress.RecorderTip <= progress.CertifiedTip {
		return progress
	}
	if decision, ok := c.decided[progress.RecorderTip]; ok {
		progress.RecorderHash = decision.Hash
		progress.RecorderHashPresent = true
		progress.RecorderHashIsCertified = true
		return progress
	}
	if recorder, ok := c.recorders[progress.RecorderTip]; ok {
		if proposal := maxProposal(recorder.FirstCurrent, recorder.AggregateCurrent, recorder.AggregatePrior); proposal != nil {
			progress.RecorderHash = proposal.Hash
			progress.RecorderHashPresent = true
		}
	}
	return progress
}
