package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

var ErrQuorumUnavailable = errors.New("QuePaxa quorum unavailable")

var (
	isrEntryMagic      = []byte("QISR1\x00")
	decisionEntryMagic = []byte("QDEC1\x00")
)

const leaderEpochSize Slot = 16

type leaderTiming struct {
	average time.Duration
	samples uint64
}

type recorderEntry struct {
	Slot  Slot `json:"slot"`
	State ISR  `json:"state"`
}

// Transport provides Algorithm 4's proposer-to-recorder RPC and decision dissemination.
type Transport interface {
	SendRecord(ctx context.Context, to NodeID, request RecordRequest) (Summary, error)
	SendDecision(ctx context.Context, decision Decision) error
}

// Core runs one QuePaxa proposer and recorder per replica.
type Core struct {
	nodeID    NodeID
	config    *Cluster
	wal       *qlog.WAL
	transport Transport
	priority  func() (Priority, error)

	slotMu     sync.Mutex
	nextSlot   Slot
	vacant     []Slot
	pipeline   chan struct{}
	mu         sync.RWMutex
	tip        Slot
	tipChanged chan struct{}
	decided    map[Slot]DecidedValue
	byHash     map[ValueHash]Slot
	recorders  map[Slot]ISR
	listeners  []chan SlotValue
	now        func() time.Time
	epochStart map[uint64]time.Time
	timings    map[NodeID]leaderTiming
}

func newCore(nodeID NodeID, config *Cluster, wal *qlog.WAL, transport Transport) *Core {
	return &Core{
		nodeID: nodeID, config: config, wal: wal, transport: transport,
		priority: randomPriority,
		nextSlot: 1, pipeline: make(chan struct{}, 16), tipChanged: make(chan struct{}),
		decided: make(map[Slot]DecidedValue), byHash: make(map[ValueHash]Slot), recorders: make(map[Slot]ISR),
		now: time.Now, epochStart: make(map[uint64]time.Time), timings: make(map[NodeID]leaderTiming),
	}
}

// Propose drives Algorithm 4. If another proposer wins this slot, the offered
// value is retried at the next slot so a successful client command is never lost.
func (c *Core) Propose(ctx context.Context, value []byte) (Slot, []Receipt, error) {
	select {
	case c.pipeline <- struct{}{}:
		defer func() { <-c.pipeline }()
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	}

	offeredHash := sha256.Sum256(value)
	for {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		slot := c.reserveSlot()
		proposed := value
		if c.isLeaderScheduleSlot(slot) {
			if err := c.WaitTip(ctx, slot-1); err != nil {
				c.releaseSlot(slot)
				return 0, nil, err
			}
			encoded, err := EncodeLeaderSchedule(c.calculateLeaderSchedule())
			if err != nil {
				c.releaseSlot(slot)
				return 0, nil, err
			}
			proposed = encoded
		}
		if err := c.waitLeaderSchedule(ctx, slot); err != nil {
			c.releaseSlot(slot)
			return 0, nil, err
		}
		c.markEpochStarted(slot)
		decision, err := c.runSlot(ctx, slot, proposed)
		if err != nil {
			c.releaseSlot(slot)
			return 0, nil, err
		}
		if err := c.AcceptDecision(decision); err != nil {
			c.releaseSlot(slot)
			return 0, nil, err
		}
		if c.transport != nil && len(c.config.Members) > 1 {
			go func(decision Decision) { _ = c.transport.SendDecision(context.Background(), decision) }(decision)
		}
		if decision.Proposal.Hash == offeredHash && bytes.Equal(decision.Proposal.Value, value) {
			return proposalResult(decision)
		}
	}
}

func proposalResult(decision Decision) (Slot, []Receipt, error) {
	receipts := make([]Receipt, 0, len(decision.Summaries))
	for _, summary := range decision.Summaries {
		receipts = append(receipts, Receipt{Slot: decision.Slot, Hash: decision.Proposal.Hash, NodeID: summary.RecorderID, Accepted: true})
	}
	return decision.Slot, receipts, nil
}

func (c *Core) decisionByValue(hash ValueHash, value []byte) (Decision, bool) {
	c.mu.RLock()
	slot, ok := c.byHash[hash]
	decided := c.decided[slot]
	c.mu.RUnlock()
	if !ok || !bytes.Equal(decided.Value, value) {
		return Decision{}, false
	}
	decision, err := decodeDecision(decided.Certificate)
	return decision, err == nil
}

func (c *Core) reserveSlot() Slot {
	c.slotMu.Lock()
	defer c.slotMu.Unlock()
	for len(c.vacant) > 0 {
		slot := c.vacant[0]
		c.vacant = c.vacant[1:]
		if !c.IsDecided(slot) {
			return slot
		}
	}
	if next := c.Tip() + 1; c.nextSlot < next {
		c.nextSlot = next
	}
	slot := c.nextSlot
	c.nextSlot++
	return slot
}

func leaderEpoch(slot Slot) uint64 {
	if slot == 0 {
		return 0
	}
	return uint64((slot - 1) / leaderEpochSize)
}

func leaderEpochFirst(epoch uint64) Slot {
	return Slot(epoch)*leaderEpochSize + 1
}

func (c *Core) explorationEpochs() uint64 {
	return uint64(2*len(c.config.Members) + 1)
}

func (c *Core) isLeaderScheduleSlot(slot Slot) bool {
	epoch := leaderEpoch(slot)
	return epoch+1 >= c.explorationEpochs() && slot == leaderEpochFirst(epoch)
}

func rotateMembers(members []Member, first int) []NodeID {
	order := make([]NodeID, 0, len(members))
	for offset := range members {
		order = append(order, members[(first+offset)%len(members)].ID)
	}
	return order
}

func (c *Core) validateLeaderSchedule(order []NodeID) bool {
	if len(order) != len(c.config.Members) {
		return false
	}
	members := c.config.MemberSet()
	seen := make(map[NodeID]struct{}, len(order))
	for _, id := range order {
		if _, ok := members[id]; !ok {
			return false
		}
		if _, duplicate := seen[id]; duplicate {
			return false
		}
		seen[id] = struct{}{}
	}
	return true
}

func (c *Core) leaderOrderLocked(slot Slot) ([]NodeID, error) {
	epoch := leaderEpoch(slot)
	if epoch < c.explorationEpochs() {
		return rotateMembers(c.config.Members, int(epoch%uint64(len(c.config.Members)))), nil
	}
	controlSlot := leaderEpochFirst(epoch - 1)
	decision, ok := c.decided[controlSlot]
	if !ok {
		return nil, fmt.Errorf("leader schedule unavailable for epoch %d", epoch)
	}
	order, schedule, err := DecodeLeaderSchedule(decision.Value)
	if err != nil || !schedule || !c.validateLeaderSchedule(order) {
		return nil, fmt.Errorf("invalid leader schedule for epoch %d", epoch)
	}
	return order, nil
}

func (c *Core) LeaderOrder(slot Slot) ([]NodeID, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.leaderOrderLocked(slot)
}

// ProposerOrder returns the agreed hedging order for the next undecided slot.
func (c *Core) ProposerOrder() []NodeID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	order, err := c.leaderOrderLocked(c.tip + 1)
	if err != nil {
		return rotateMembers(c.config.Members, 0)
	}
	return order
}

func (c *Core) waitLeaderSchedule(ctx context.Context, slot Slot) error {
	epoch := leaderEpoch(slot)
	if epoch < c.explorationEpochs() {
		return nil
	}
	return c.WaitTip(ctx, leaderEpochFirst(epoch-1))
}

func (c *Core) markEpochStarted(slot Slot) {
	epoch := leaderEpoch(slot)
	c.mu.Lock()
	if _, ok := c.epochStart[epoch]; !ok {
		c.epochStart[epoch] = c.now()
	}
	c.mu.Unlock()
}

func (c *Core) calculateLeaderSchedule() []NodeID {
	c.mu.RLock()
	defer c.mu.RUnlock()
	order, err := c.leaderOrderLocked(c.tip)
	if err != nil {
		order = rotateMembers(c.config.Members, 0)
	}
	position := make(map[NodeID]int, len(order))
	for i, id := range order {
		position[id] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		left, leftOK := c.timings[order[i]]
		right, rightOK := c.timings[order[j]]
		if !leftOK || !rightOK || left.samples == 0 || right.samples == 0 {
			return position[order[i]] < position[order[j]]
		}
		if left.average == right.average {
			return position[order[i]] < position[order[j]]
		}
		return left.average < right.average
	})
	return order
}

func (c *Core) releaseSlot(slot Slot) {
	if slot == 0 || c.IsDecided(slot) {
		return
	}
	c.slotMu.Lock()
	defer c.slotMu.Unlock()
	index := sort.Search(len(c.vacant), func(i int) bool { return c.vacant[i] >= slot })
	if index < len(c.vacant) && c.vacant[index] == slot {
		return
	}
	c.vacant = append(c.vacant, 0)
	copy(c.vacant[index+1:], c.vacant[index:])
	c.vacant[index] = slot
}

func (c *Core) runSlot(ctx context.Context, slot Slot, value []byte) (Decision, error) {
	leaderOrder, err := c.LeaderOrder(slot)
	if err != nil {
		return Decision{}, err
	}
	leader := leaderOrder[0]
	proposal := newProposal(highestPriority, c.nodeID, value)
	step := Step(4)
	for {
		if decided, ok := c.decision(slot); ok {
			return decodeDecision(decided.Certificate)
		}

		requests := make(map[NodeID]RecordRequest, len(c.config.Members))
		for _, member := range c.config.Members {
			candidate := proposal
			if step%4 == 0 && (step > 4 || c.nodeID != leader) {
				priority, err := c.priority()
				if err != nil {
					return Decision{}, err
				}
				candidate.Priority = priority
			}
			requests[member.ID] = RecordRequest{Slot: slot, Step: step, Proposal: candidate}
		}

		summaries, err := c.recordQuorum(ctx, requests)
		if err != nil {
			return Decision{}, err
		}
		sort.Slice(summaries, func(i, j int) bool { return summaries[i].RecorderID < summaries[j].RecorderID })

		allCurrent := true
		for _, summary := range summaries {
			if summary.Step != step {
				allCurrent = false
				break
			}
		}
		if !allCurrent {
			var caught *Summary
			for i := range summaries {
				if summaries[i].Step > step && (caught == nil || summaries[i].Step > caught.Step) {
					caught = &summaries[i]
				}
			}
			if caught == nil || caught.FirstCurrent == nil {
				return Decision{}, fmt.Errorf("invalid QuePaxa catch-up at slot %d step %d", slot, step)
			}
			step, proposal = caught.Step, *cloneProposal(caught.FirstCurrent)
			continue
		}

		switch step % 4 {
		case 0:
			fast := step == 4
			var first *Proposal
			for _, summary := range summaries {
				if summary.FirstCurrent == nil || summary.FirstCurrent.Priority != highestPriority {
					fast = false
				}
				if first == nil {
					first = summary.FirstCurrent
				} else if fast && !sameProposal(first, summary.FirstCurrent) {
					return Decision{}, fmt.Errorf("multiple highest-priority proposals at slot %d", slot)
				}
			}
			if fast {
				return Decision{Slot: slot, Step: step, Proposal: *cloneProposal(first), Summaries: summaries}, nil
			}
			best := maxFirst(summaries)
			if best == nil {
				return Decision{}, fmt.Errorf("empty QuePaxa proposal set at slot %d step %d", slot, step)
			}
			proposal = *best
		case 1:
			// Algorithm 4 has no phase-specific action.
		case 2:
			if sameProposal(&proposal, maxPrior(summaries)) {
				return Decision{Slot: slot, Step: step, Proposal: proposal, Summaries: summaries}, nil
			}
		case 3:
			best := maxPrior(summaries)
			if best == nil {
				return Decision{}, fmt.Errorf("empty QuePaxa common set at slot %d step %d", slot, step)
			}
			proposal = *best
		}
		step++
		if step < 4 {
			return Decision{}, fmt.Errorf("QuePaxa logical step overflow")
		}
	}
}

func maxFirst(summaries []Summary) *Proposal {
	var best *Proposal
	for _, summary := range summaries {
		best = maxProposal(best, summary.FirstCurrent)
	}
	return best
}

func maxPrior(summaries []Summary) *Proposal {
	var best *Proposal
	for _, summary := range summaries {
		best = maxProposal(best, summary.AggregatePrior)
	}
	return best
}

func (c *Core) recordQuorum(ctx context.Context, requests map[NodeID]RecordRequest) ([]Summary, error) {
	type result struct {
		summary Summary
		err     error
	}
	results := make(chan result, len(requests))
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for nodeID, request := range requests {
		go func(nodeID NodeID, request RecordRequest) {
			var summary Summary
			var err error
			if nodeID == c.nodeID {
				summary, err = c.Record(callCtx, request)
			} else if c.transport == nil {
				err = fmt.Errorf("no transport for %s", nodeID)
			} else {
				summary, err = c.transport.SendRecord(callCtx, nodeID, request)
			}
			results <- result{summary: summary, err: err}
		}(nodeID, request)
	}

	quorum := c.config.QuorumSize()
	summaries := make([]Summary, 0, quorum)
	var firstErr error
	for completed := 0; completed < len(requests); completed++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-results:
			if result.err == nil {
				summaries = append(summaries, result.summary)
				if len(summaries) == quorum {
					return summaries, nil
				}
			} else if firstErr == nil {
				firstErr = result.err
			}
		}
	}
	if firstErr != nil {
		return nil, fmt.Errorf("%w: %v", ErrQuorumUnavailable, firstErr)
	}
	return nil, ErrQuorumUnavailable
}

// Record durably applies the paper's Algorithm 3 before replying.
func (c *Core) Record(_ context.Context, request RecordRequest) (Summary, error) {
	if request.Slot == 0 || request.Step < 4 {
		return Summary{}, fmt.Errorf("invalid QuePaxa slot or step")
	}
	if sha256.Sum256(request.Proposal.Value) != request.Proposal.Hash {
		return Summary{}, fmt.Errorf("proposal hash mismatch")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if decided, ok := c.decided[request.Slot]; ok {
		decision, err := decodeDecision(decided.Certificate)
		if err != nil {
			return Summary{}, err
		}
		step := decision.Step
		if request.Step > step {
			step = request.Step
		}
		return Summary{
			RecorderID: c.nodeID, Step: step, FirstCurrent: cloneProposal(&decision.Proposal),
			AggregatePrior: cloneProposal(&decision.Proposal),
		}, nil
	}
	epoch := leaderEpoch(request.Slot)
	if _, ok := c.epochStart[epoch]; !ok {
		c.epochStart[epoch] = c.now()
	}
	next, summary := c.recorders[request.Slot].Record(request.Step, request.Proposal)
	summary.RecorderID = c.nodeID
	payload, err := json.Marshal(recorderEntry{Slot: request.Slot, State: next})
	if err != nil {
		return Summary{}, err
	}
	if err := c.wal.Append(qlog.Entry{Slot: uint64(request.Slot), Hash: request.Proposal.Hash, Type: qlog.EntryReceipt, Payload: append(isrEntryMagic, payload...)}); err != nil {
		return Summary{}, err
	}
	if err := c.wal.Sync(); err != nil {
		return Summary{}, err
	}
	c.recorders[request.Slot] = next
	return summary, nil
}

// AcceptDecision validates Algorithm 4 quorum evidence and records the decision durably.
func (c *Core) AcceptDecision(decision Decision) error {
	if err := c.validateDecision(decision); err != nil {
		return err
	}
	certificate, err := json.Marshal(decision)
	if err != nil {
		return err
	}
	payload := append(append([]byte(nil), decisionEntryMagic...), certificate...)

	c.mu.Lock()
	if existing, ok := c.decided[decision.Slot]; ok {
		c.mu.Unlock()
		if existing.Hash != decision.Proposal.Hash || !bytes.Equal(existing.Value, decision.Proposal.Value) {
			return fmt.Errorf("slot %d already decided with another value", decision.Slot)
		}
		return nil
	}
	if err := c.wal.Append(qlog.Entry{Slot: uint64(decision.Slot), Hash: decision.Proposal.Hash, Type: qlog.EntryDecide, Payload: payload}); err != nil {
		c.mu.Unlock()
		return err
	}
	if err := c.wal.Sync(); err != nil {
		c.mu.Unlock()
		return err
	}
	value := DecidedValue{Slot: decision.Slot, Hash: decision.Proposal.Hash, Value: append([]byte(nil), decision.Proposal.Value...), Certificate: certificate}
	c.decided[decision.Slot] = value
	c.byHash[decision.Proposal.Hash] = decision.Slot
	delete(c.recorders, decision.Slot)
	c.advanceTipLocked()
	listeners := append([]chan SlotValue(nil), c.listeners...)
	c.mu.Unlock()

	for _, listener := range listeners {
		select {
		case listener <- SlotValue{Slot: decision.Slot, Hash: decision.Proposal.Hash}:
		default:
		}
	}
	return nil
}

func (c *Core) validateDecision(decision Decision) error {
	if decision.Slot == 0 || sha256.Sum256(decision.Proposal.Value) != decision.Proposal.Hash {
		return fmt.Errorf("invalid QuePaxa decision value")
	}
	if order, schedule, err := DecodeLeaderSchedule(decision.Proposal.Value); err != nil {
		return fmt.Errorf("decode leader schedule: %w", err)
	} else if schedule && (!c.isLeaderScheduleSlot(decision.Slot) || !c.validateLeaderSchedule(order)) {
		return fmt.Errorf("invalid QuePaxa leader schedule")
	}
	if _, err := DecodeReadBarrier(decision.Proposal.Value); err != nil {
		return fmt.Errorf("decode read barrier: %w", err)
	}
	members := c.config.MemberSet()
	seen := make(map[NodeID]struct{}, len(decision.Summaries))
	for _, summary := range decision.Summaries {
		if _, ok := members[summary.RecorderID]; !ok {
			return fmt.Errorf("decision contains non-member recorder %q", summary.RecorderID)
		}
		if _, duplicate := seen[summary.RecorderID]; duplicate {
			return fmt.Errorf("decision contains duplicate recorder %q", summary.RecorderID)
		}
		seen[summary.RecorderID] = struct{}{}
		if summary.Step != decision.Step {
			return fmt.Errorf("decision mixes QuePaxa steps")
		}
	}
	if len(seen) < c.config.QuorumSize() {
		return ErrQuorumUnavailable
	}

	switch decision.Step % 4 {
	case 0:
		order, err := c.LeaderOrder(decision.Slot)
		if err != nil {
			return err
		}
		if decision.Step != 4 || decision.Proposal.ProposerID != order[0] || decision.Proposal.Priority != highestPriority {
			return fmt.Errorf("invalid QuePaxa fast-path decision")
		}
		for _, summary := range decision.Summaries {
			if summary.FirstCurrent == nil || summary.FirstCurrent.Priority != highestPriority || !sameProposal(summary.FirstCurrent, &decision.Proposal) {
				return fmt.Errorf("invalid QuePaxa fast-path quorum")
			}
		}
	case 2:
		if !sameProposal(maxPrior(decision.Summaries), &decision.Proposal) {
			return fmt.Errorf("invalid QuePaxa phase-2 decision")
		}
	default:
		return fmt.Errorf("QuePaxa cannot decide in phase %d", decision.Step%4)
	}
	return nil
}

func (c *Core) decision(slot Slot) (DecidedValue, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	decision, ok := c.decided[slot]
	return decision, ok
}

func decodeDecision(certificate []byte) (Decision, error) {
	var decision Decision
	if len(certificate) == 0 {
		return Decision{}, fmt.Errorf("decision has no QuePaxa certificate")
	}
	if err := json.Unmarshal(certificate, &decision); err != nil {
		return Decision{}, fmt.Errorf("decode QuePaxa certificate: %w", err)
	}
	return decision, nil
}

// AcceptCertifiedValue binds catch-up metadata to its certificate before mutation.
func (c *Core) AcceptCertifiedValue(value DecidedValue) error {
	decision, err := decodeDecision(value.Certificate)
	if err != nil {
		return err
	}
	if decision.Slot != value.Slot || decision.Proposal.Hash != value.Hash || !bytes.Equal(decision.Proposal.Value, value.Value) {
		return fmt.Errorf("catch-up value does not match QuePaxa certificate")
	}
	return c.AcceptDecision(decision)
}

// CertifiedValue returns a decided value even if an earlier slot is still missing.
func (c *Core) CertifiedValue(slot Slot) (DecidedValue, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.decided[slot]
	value.Value = append([]byte(nil), value.Value...)
	value.Certificate = append([]byte(nil), value.Certificate...)
	return value, ok
}

func (c *Core) DecidedSlot(value []byte) (Slot, bool) {
	decision, ok := c.decisionByValue(sha256.Sum256(value), value)
	return decision.Slot, ok
}

// AcceptLearned is retained only for single-node compatibility. Clustered
// callers must use AcceptDecision so learned hints cannot bypass consensus.
func (c *Core) AcceptLearned(slot Slot, value []byte, hash ValueHash) error {
	if len(c.config.Members) != 1 {
		return fmt.Errorf("uncertified learned decision rejected")
	}
	proposal := newProposal(highestPriority, c.nodeID, value)
	if proposal.Hash != hash {
		return fmt.Errorf("learned value hash mismatch")
	}
	summary := Summary{RecorderID: c.nodeID, Step: 4, FirstCurrent: cloneProposal(&proposal)}
	return c.AcceptDecision(Decision{Slot: slot, Step: 4, Proposal: proposal, Summaries: []Summary{summary}})
}

func (c *Core) Learn(ctx context.Context, from Slot) (<-chan SlotValue, error) {
	ch := make(chan SlotValue, 100)
	c.mu.Lock()
	c.listeners = append(c.listeners, ch)
	for slot, value := range c.decided {
		if slot >= from {
			select {
			case ch <- SlotValue{Slot: slot, Hash: value.Hash}:
			default:
			}
		}
	}
	c.mu.Unlock()
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		for i, listener := range c.listeners {
			if listener == ch {
				c.listeners = append(c.listeners[:i], c.listeners[i+1:]...)
				break
			}
		}
		c.mu.Unlock()
	}()
	return ch, nil
}

func (c *Core) DecisionsFrom(from Slot, limit int) ([]DecidedValue, Slot, error) {
	if from == 0 {
		from = 1
	}
	if limit <= 0 {
		limit = 256
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	page := make([]DecidedValue, 0, limit)
	for slot := from; slot <= c.tip && len(page) < limit; slot++ {
		decision, ok := c.decided[slot]
		if !ok {
			return nil, 0, fmt.Errorf("decision gap at slot %d", slot)
		}
		decision.Value = append([]byte(nil), decision.Value...)
		decision.Certificate = append([]byte(nil), decision.Certificate...)
		page = append(page, decision)
	}
	return page, c.tip, nil
}

func (c *Core) Tip() Slot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.tip
}

// WaitTip waits until all decisions through slot are contiguous.
func (c *Core) WaitTip(ctx context.Context, slot Slot) error {
	for {
		c.mu.RLock()
		if c.tip >= slot {
			c.mu.RUnlock()
			return nil
		}
		changed := c.tipChanged
		c.mu.RUnlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (c *Core) NodeID() NodeID { return c.nodeID }
func (c *Core) ConfigID() uint { return c.config.ConfigID }

func (c *Core) IsDecided(slot Slot) bool {
	_, ok := c.decision(slot)
	return ok
}

func (c *Core) IsQuorum(receipts []Receipt) bool {
	seen := make(map[NodeID]struct{}, len(receipts))
	members := c.config.MemberSet()
	for _, receipt := range receipts {
		if receipt.Accepted {
			if _, ok := members[receipt.NodeID]; ok {
				seen[receipt.NodeID] = struct{}{}
			}
		}
	}
	return len(seen) >= c.config.QuorumSize()
}

func (c *Core) recover() error {
	entries, err := c.wal.Read()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		switch entry.Type {
		case qlog.EntryReceipt:
			if !bytes.HasPrefix(entry.Payload, isrEntryMagic) {
				continue
			}
			var persisted recorderEntry
			if err := json.Unmarshal(entry.Payload[len(isrEntryMagic):], &persisted); err != nil {
				return fmt.Errorf("recover QuePaxa ISR: %w", err)
			}
			c.mu.Lock()
			if _, decided := c.decided[persisted.Slot]; !decided {
				c.recorders[persisted.Slot] = persisted.State
			}
			c.mu.Unlock()
		case qlog.EntryDecide:
			if !bytes.HasPrefix(entry.Payload, decisionEntryMagic) {
				continue
			}
			certificate := entry.Payload[len(decisionEntryMagic):]
			decision, err := decodeDecision(certificate)
			if err != nil {
				return err
			}
			if err := c.validateDecision(decision); err != nil {
				return fmt.Errorf("recover QuePaxa decision: %w", err)
			}
			c.mu.Lock()
			if existing, ok := c.decided[decision.Slot]; ok && existing.Hash != decision.Proposal.Hash {
				c.mu.Unlock()
				return fmt.Errorf("conflicting decisions at slot %d", decision.Slot)
			}
			c.decided[decision.Slot] = DecidedValue{Slot: decision.Slot, Hash: decision.Proposal.Hash, Value: decision.Proposal.Value, Certificate: certificate}
			c.byHash[decision.Proposal.Hash] = decision.Slot
			delete(c.recorders, decision.Slot)
			c.mu.Unlock()
		}
	}
	c.mu.Lock()
	c.advanceTipLocked()
	c.mu.Unlock()
	return nil
}

func (c *Core) advanceTipLocked() {
	before := c.tip
	for {
		if _, ok := c.decided[c.tip+1]; !ok {
			break
		}
		c.tip++
		if c.tip%leaderEpochSize == 0 {
			epoch := leaderEpoch(c.tip)
			if started, ok := c.epochStart[epoch]; ok {
				if order, err := c.leaderOrderLocked(c.tip); err == nil {
					timing := c.timings[order[0]]
					duration := c.now().Sub(started)
					if timing.samples == 0 {
						timing.average = duration
					} else {
						timing.average = (timing.average + duration) / 2
					}
					timing.samples++
					c.timings[order[0]] = timing
				}
				delete(c.epochStart, epoch)
			}
		}
	}
	if c.tip != before {
		close(c.tipChanged)
		c.tipChanged = make(chan struct{})
	}
}

func (c *Core) StartPeriodicSync(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = c.wal.Sync()
			}
		}
	}()
}
