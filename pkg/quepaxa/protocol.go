package quepaxa

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
)

// Step is QuePaxa's logical clock. Four consecutive steps form one round.
type Step uint64

// Priority is the 256-bit proposal priority from Algorithm 4.
type Priority [32]byte

var highestPriority = Priority{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

// Proposal is the paper's lexicographically ordered <priority, proposer, value> tuple.
type Proposal struct {
	Priority   Priority  `json:"priority"`
	ProposerID NodeID    `json:"proposer_id"`
	Hash       ValueHash `json:"hash"`
	Value      []byte    `json:"value"`
}

func newProposal(priority Priority, proposerID NodeID, value []byte) Proposal {
	return Proposal{Priority: priority, ProposerID: proposerID, Hash: sha256.Sum256(value), Value: append([]byte(nil), value...)}
}

func randomPriority() (Priority, error) {
	var priority Priority
	if _, err := rand.Read(priority[:]); err != nil {
		return Priority{}, fmt.Errorf("QuePaxa priority: %w", err)
	}
	if priority == (Priority{}) || priority == highestPriority {
		priority[31] = 1
	}
	return priority, nil
}

func compareProposal(left, right *Proposal) int {
	if left == nil {
		if right == nil {
			return 0
		}
		return -1
	}
	if right == nil {
		return 1
	}
	if result := bytes.Compare(left.Priority[:], right.Priority[:]); result != 0 {
		return result
	}
	if result := bytes.Compare([]byte(left.ProposerID), []byte(right.ProposerID)); result != 0 {
		return result
	}
	return bytes.Compare(left.Value, right.Value)
}

func maxProposal(values ...*Proposal) *Proposal {
	var best *Proposal
	for _, value := range values {
		if compareProposal(value, best) > 0 {
			best = cloneProposal(value)
		}
	}
	return best
}

func sameProposal(left, right *Proposal) bool {
	return compareProposal(left, right) == 0
}

func cloneProposal(proposal *Proposal) *Proposal {
	if proposal == nil {
		return nil
	}
	copy := *proposal
	copy.Value = append([]byte(nil), proposal.Value...)
	return &copy
}

// ISR is the constant-space interval summary register in Algorithm 3.
type ISR struct {
	Step             Step      `json:"step"`
	FirstCurrent     *Proposal `json:"first_current,omitempty"`
	AggregateCurrent *Proposal `json:"aggregate_current,omitempty"`
	AggregatePrior   *Proposal `json:"aggregate_prior,omitempty"`
}

// Record applies Algorithm 3. Stale inputs return the current summary unchanged.
func (state ISR) Record(step Step, proposal Proposal) (ISR, Summary) {
	next := state
	if step == next.Step {
		next.AggregateCurrent = maxProposal(next.AggregateCurrent, &proposal)
	} else if step > next.Step {
		if step == next.Step+1 {
			next.AggregatePrior = cloneProposal(next.AggregateCurrent)
		} else {
			next.AggregatePrior = nil
		}
		next.Step = step
		next.FirstCurrent = cloneProposal(&proposal)
		next.AggregateCurrent = cloneProposal(&proposal)
	}
	return next, Summary{Step: next.Step, FirstCurrent: cloneProposal(next.FirstCurrent), AggregatePrior: cloneProposal(next.AggregatePrior)}
}

// RecordRequest and Summary are the proposer-to-recorder RPC values in Algorithms 3 and 4.
type RecordRequest struct {
	Slot     Slot     `json:"slot"`
	Step     Step     `json:"step"`
	Proposal Proposal `json:"proposal"`
}

type Summary struct {
	RecorderID     NodeID    `json:"recorder_id"`
	Step           Step      `json:"step"`
	FirstCurrent   *Proposal `json:"first_current,omitempty"`
	AggregatePrior *Proposal `json:"aggregate_prior,omitempty"`
}

// Decision carries the quorum evidence for an Algorithm 4 decision.
type Decision struct {
	Slot      Slot      `json:"slot"`
	Step      Step      `json:"step"`
	Proposal  Proposal  `json:"proposal"`
	Summaries []Summary `json:"summaries"`
}
