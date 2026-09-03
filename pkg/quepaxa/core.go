package quepaxa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

var ErrQuorumUnavailable = errors.New("QuePaxa quorum unavailable")
var ErrCompacted = errors.New("QuePaxa history compacted")

var (
	isrEntryMagic      = []byte("QISR\x00")
	decisionEntryMagic = []byte("QDEC\x00")
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
	ReadTip(ctx context.Context, to NodeID) (Slot, error)
	StageValue(ctx context.Context, to NodeID, hash ValueHash, value []byte) error
	FetchValue(ctx context.Context, from NodeID, hash ValueHash) ([]byte, error)
}

// Core runs one QuePaxa proposer and recorder per replica.
type Core struct {
	nodeID    NodeID
	config    *Cluster
	wal       *qlog.WAL
	transport Transport
	observer  bool
	priority  func() (Priority, error)

	slotMu              sync.Mutex
	nextSlot            Slot
	vacant              []Slot
	pipeline            chan struct{}
	mu                  sync.RWMutex
	tip                 Slot
	floor               Slot
	floorRoot           [32]byte
	baseLeaderEpoch     uint64
	baseLeaderOrder     []NodeID
	baseFollowingEpoch  uint64
	baseFollowingOrder  []NodeID
	tipChanged          chan struct{}
	decided             map[Slot]DecidedValue
	durable             map[Slot]bool
	logged              map[Slot]bool
	byHash              map[ValueHash]Slot
	values              map[ValueHash][]byte
	valueDurable        map[ValueHash]bool
	compactionValues    map[[32]byte]struct{}
	prefixes            map[Slot][32]byte
	checkpointMu        sync.Mutex
	checkpointValidator func(context.Context, CheckpointSeal) error
	preparedCheckpoints map[Slot][32]byte
	sealedRoots         map[[32]byte]SealedCheckpoint
	recorders           map[Slot]ISR
	recordLocks         [64]sync.Mutex
	commits             *groupCommit
	listeners           []chan SlotValue
	now                 func() time.Time
	epochStart          map[uint64]time.Time
	timings             map[NodeID]leaderTiming
	periodicMu          sync.Mutex
	periodic            context.CancelFunc
	periodicWG          sync.WaitGroup
	healthMu            sync.Mutex
	periodicErr         error
}

func newCore(nodeID NodeID, config *Cluster, wal *qlog.WAL, transport Transport) *Core {
	return &Core{
		nodeID: nodeID, config: config, wal: wal, transport: transport,
		priority: randomPriority,
		nextSlot: 1, pipeline: make(chan struct{}, 16), tipChanged: make(chan struct{}),
		decided: make(map[Slot]DecidedValue), durable: make(map[Slot]bool), logged: make(map[Slot]bool), byHash: make(map[ValueHash]Slot), values: make(map[ValueHash][]byte), valueDurable: make(map[ValueHash]bool), prefixes: make(map[Slot][32]byte), preparedCheckpoints: make(map[Slot][32]byte), sealedRoots: make(map[[32]byte]SealedCheckpoint), recorders: make(map[Slot]ISR),
		now: time.Now, epochStart: make(map[uint64]time.Time), timings: make(map[NodeID]leaderTiming), commits: newGroupCommit(wal.Sync),
	}
}

// Propose drives Algorithm 4. If another proposer wins this slot, the offered
// value is retried at the next slot so a successful client command is never lost.
func (c *Core) Propose(ctx context.Context, value []byte) (Slot, []Receipt, error) {
	return c.propose(ctx, value, true)
}

// ProposeCertified returns after a recorder quorum has durably certified the
// value. The caller must install the returned decision on another voter before
// acknowledging the client.
func (c *Core) ProposeCertified(ctx context.Context, value []byte) (Slot, []Receipt, error) {
	return c.propose(ctx, value, false)
}

func (c *Core) propose(ctx context.Context, value []byte, complete bool) (Slot, []Receipt, error) {
	if c.observer {
		return 0, nil, ErrQuorumUnavailable
	}
	if len(value) > MaxReplicatedValueBytes {
		return 0, nil, fmt.Errorf("QuePaxa value exceeds %d bytes", MaxReplicatedValueBytes)
	}
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
		slot, reused := c.reserveSlot()
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
		decision, err := c.runSlot(ctx, slot, proposed, !reused)
		if err != nil {
			c.releaseSlot(slot)
			return slot, nil, err
		}
		// A durable recorder quorum already makes the decision recoverable. Like
		// Raft's commit index, the local decision marker need not add a second
		// synchronous disk barrier to the clustered fast path.
		if err := c.acceptDecision(decision); err != nil {
			c.releaseSlot(slot)
			return slot, nil, err
		}
		if complete {
			if _, err := c.completeDecision(ctx, decision.Slot, len(c.config.Members) == 1); err != nil {
				return decision.Slot, nil, err
			}
		}
		if decision.Proposal.Hash == offeredHash && bytes.Equal(decision.Proposal.Value, value) {
			return proposalResult(decision)
		}
	}
}

func (c *Core) StageValue(hash ValueHash, value []byte) error {
	if len(value) == 0 || len(value) > MaxReplicatedValueBytes || sha256.Sum256(value) != hash {
		return fmt.Errorf("invalid QuePaxa value")
	}
	c.mu.Lock()
	if existing, ok := c.values[hash]; ok {
		if !bytes.Equal(existing, value) {
			c.mu.Unlock()
			return fmt.Errorf("QuePaxa value hash collision")
		}
		if c.compactionValues != nil {
			key := [32]byte(hash)
			if _, retained := c.compactionValues[key]; !retained {
				if err := c.wal.Append(qlog.Entry{Hash: hash, Type: qlog.EntryProposal, Payload: append([]byte(nil), value...)}); err != nil {
					c.mu.Unlock()
					return err
				}
				c.compactionValues[key] = struct{}{}
			}
		}
		c.mu.Unlock()
		return nil
	}
	if err := c.wal.Append(qlog.Entry{Hash: hash, Type: qlog.EntryProposal, Payload: append([]byte(nil), value...)}); err != nil {
		c.mu.Unlock()
		return err
	}
	c.values[hash] = append([]byte(nil), value...)
	if c.compactionValues != nil {
		c.compactionValues[[32]byte(hash)] = struct{}{}
	}
	c.mu.Unlock()
	return nil
}

// StoreValue durably installs one content-addressed proposal value.
func (c *Core) StoreValue(hash ValueHash, value []byte) error {
	if err := c.StageValue(hash, value); err != nil {
		return err
	}
	c.mu.RLock()
	durable := c.valueDurable[hash]
	c.mu.RUnlock()
	if durable {
		return nil
	}
	if err := c.commits.Sync(context.Background()); err != nil {
		return err
	}
	c.mu.Lock()
	c.valueDurable[hash] = true
	c.mu.Unlock()
	return nil
}

func (c *Core) Value(hash ValueHash) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.values[hash]
	ok = ok && c.valueDurable[hash]
	return append([]byte(nil), value...), ok
}

func (c *Core) stagedValue(hash ValueHash) ([]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, ok := c.values[hash]
	return append([]byte(nil), value...), ok
}

func (c *Core) hydrateProposal(ctx context.Context, proposal *Proposal, sources ...NodeID) error {
	if proposal == nil || len(proposal.Value) != 0 {
		return nil
	}
	if value, ok := c.stagedValue(proposal.Hash); ok {
		proposal.Value = value
		return nil
	}
	transport := c.transport
	if transport == nil {
		return fmt.Errorf("value %x is unavailable", proposal.Hash[:8])
	}
	for _, source := range sources {
		value, err := transport.FetchValue(ctx, source, proposal.Hash)
		if err != nil || sha256.Sum256(value) != proposal.Hash {
			continue
		}
		if err := c.StoreValue(proposal.Hash, value); err != nil {
			return err
		}
		proposal.Value = value
		return nil
	}
	return fmt.Errorf("value %x is unavailable", proposal.Hash[:8])
}

// ReadIndex returns a certified tip observed by a quorum. Writes are exposed
// only after a learner quorum has accepted their decision, so the two quorums
// intersect and a completed write cannot be missed.
func (c *Core) ReadIndex(ctx context.Context) (Slot, NodeID, error) {
	if c.observer {
		return 0, "", ErrQuorumUnavailable
	}
	if len(c.config.Members) == 1 {
		return c.Tip(), c.nodeID, nil
	}
	transport := c.transport
	if transport == nil {
		return 0, "", fmt.Errorf("%w: read-index transport is unavailable", ErrQuorumUnavailable)
	}
	type result struct {
		tip Slot
		id  NodeID
		err error
	}
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan result, len(c.config.Members))
	for _, member := range c.config.Members {
		if member.ID == c.nodeID {
			results <- result{tip: c.Tip(), id: c.nodeID}
			continue
		}
		go func(id NodeID) {
			tip, err := transport.ReadTip(readCtx, id)
			results <- result{tip: tip, id: id, err: err}
		}(member.ID)
	}
	var best result
	successes := 0
	for completed := 0; completed < len(c.config.Members); completed++ {
		select {
		case <-ctx.Done():
			return 0, "", ctx.Err()
		case item := <-results:
			if item.err != nil {
				continue
			}
			successes++
			if item.tip > best.tip || best.id == "" {
				best = item
			}
			if successes == c.config.QuorumSize() {
				return best.tip, best.id, nil
			}
		}
	}
	return 0, "", ErrQuorumUnavailable
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
	decision.Proposal.Value = append([]byte(nil), decided.Value...)
	return decision, err == nil
}

func (c *Core) reserveSlot() (Slot, bool) {
	c.slotMu.Lock()
	defer c.slotMu.Unlock()
	c.mu.RLock()
	defer c.mu.RUnlock()
	for len(c.vacant) > 0 {
		slot := c.vacant[0]
		c.vacant = c.vacant[1:]
		if slot > c.floor {
			if _, decided := c.decided[slot]; !decided {
				return slot, true
			}
		}
	}
	next := c.tip + 1
	if floorNext := c.floor + 1; next < floorNext {
		next = floorNext
	}
	if c.nextSlot < next {
		c.nextSlot = next
	}
	slot := c.nextSlot
	c.nextSlot++
	return slot, false
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
		if epoch == c.baseLeaderEpoch && len(c.baseLeaderOrder) != 0 {
			return append([]NodeID(nil), c.baseLeaderOrder...), nil
		}
		if epoch == c.baseFollowingEpoch && len(c.baseFollowingOrder) != 0 {
			return append([]NodeID(nil), c.baseFollowingOrder...), nil
		}
		return nil, fmt.Errorf("leader schedule unavailable for epoch %d", epoch)
	}
	order, schedule, err := DecodeLeaderSchedule(decision.Value)
	if err != nil || !schedule || !c.validateLeaderSchedule(order) {
		return nil, fmt.Errorf("invalid leader schedule for epoch %d", epoch)
	}
	return order, nil
}

func (c *Core) checkpointNeedsFollowingOrder(index Slot) bool {
	epoch := leaderEpoch(index + 1)
	return epoch+1 >= c.explorationEpochs() && leaderEpochFirst(epoch) <= index
}

func (c *Core) validateCheckpointLeaderOrders(index Slot, next, following []NodeID) bool {
	if !c.validateLeaderSchedule(next) {
		return false
	}
	if c.checkpointNeedsFollowingOrder(index) {
		return c.validateLeaderSchedule(following)
	}
	return len(following) == 0
}

func (c *Core) checkpointLeaderOrdersLocked(index Slot) ([]NodeID, []NodeID, error) {
	next, err := c.leaderOrderLocked(index + 1)
	if err != nil {
		return nil, nil, err
	}
	if !c.checkpointNeedsFollowingOrder(index) {
		return next, nil, nil
	}
	following, err := c.leaderOrderLocked(leaderEpochFirst(leaderEpoch(index+1) + 1))
	return next, following, err
}

func (c *Core) CheckpointLeaderOrders(index Slot) ([]NodeID, []NodeID, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.checkpointLeaderOrdersLocked(index)
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
	if slot == 0 {
		return
	}
	c.slotMu.Lock()
	defer c.slotMu.Unlock()
	c.mu.RLock()
	compacted := slot <= c.floor
	_, decided := c.decided[slot]
	c.mu.RUnlock()
	if compacted || decided {
		return
	}
	index := sort.Search(len(c.vacant), func(i int) bool { return c.vacant[i] >= slot })
	if index < len(c.vacant) && c.vacant[index] == slot {
		return
	}
	c.vacant = append(c.vacant, 0)
	copy(c.vacant[index+1:], c.vacant[index:])
	c.vacant[index] = slot
}

func (c *Core) runSlot(ctx context.Context, slot Slot, value []byte, allowLeaderFastPath bool) (Decision, error) {
	leaderOrder, err := c.LeaderOrder(slot)
	if err != nil {
		return Decision{}, err
	}
	leader := leaderOrder[0]
	proposal := newProposal(highestPriority, c.nodeID, value)
	step := Step(4)
	for {
		if decided, ok := c.decision(slot); ok {
			decision, err := decodeDecision(decided.Certificate)
			decision.Proposal.Value = append([]byte(nil), decided.Value...)
			return decision, err
		}

		candidate := proposal
		if step%4 == 0 && (step > 4 || c.nodeID != leader || !allowLeaderFastPath) {
			priority, err := c.priority()
			if err != nil {
				return Decision{}, err
			}
			candidate.Priority = priority
		}
		requests := make(map[NodeID]RecordRequest, len(c.config.Members))
		for _, member := range c.config.Members {
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
			if err := c.hydrateProposal(ctx, &proposal, caught.RecorderID); err != nil {
				return Decision{}, err
			}
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
				proposal = *cloneProposal(first)
				if err := c.hydrateProposal(ctx, &proposal, summarySources(summaries)...); err != nil {
					return Decision{}, err
				}
				return Decision{Slot: slot, Step: step, Proposal: proposal, Summaries: summaries}, nil
			}
			best := maxFirst(summaries)
			if best == nil {
				return Decision{}, fmt.Errorf("empty QuePaxa proposal set at slot %d step %d", slot, step)
			}
			proposal = *best
			if err := c.hydrateProposal(ctx, &proposal, summarySources(summaries)...); err != nil {
				return Decision{}, err
			}
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
			if err := c.hydrateProposal(ctx, &proposal, summarySources(summaries)...); err != nil {
				return Decision{}, err
			}
		}
		step++
		if step < 4 {
			return Decision{}, fmt.Errorf("QuePaxa logical step overflow")
		}
	}
}

func summarySources(summaries []Summary) []NodeID {
	sources := make([]NodeID, len(summaries))
	for i := range summaries {
		sources[i] = summaries[i].RecorderID
	}
	return sources
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
func (c *Core) Record(ctx context.Context, request RecordRequest) (Summary, error) {
	if c.observer {
		return Summary{}, ErrQuorumUnavailable
	}
	if request.Slot == 0 || request.Step < 4 {
		return Summary{}, fmt.Errorf("invalid QuePaxa slot or step")
	}
	if err := c.rejectCompacted(request.Slot); err != nil {
		return Summary{}, err
	}
	if len(request.Proposal.Value) != 0 && len(request.Proposal.Value) <= MaxReplicatedValueBytes {
		if err := c.StageValue(request.Proposal.Hash, request.Proposal.Value); err != nil {
			return Summary{}, err
		}
	}
	if err := c.validateCheckpointValue(ctx, request.Proposal.Hash); err != nil {
		return Summary{}, err
	}
	lock := &c.recordLocks[uint64(request.Slot)%uint64(len(c.recordLocks))]
	lock.Lock()
	defer lock.Unlock()
	c.mu.Lock()
	if err := c.compactedErrorLocked(request.Slot); err != nil {
		c.mu.Unlock()
		return Summary{}, err
	}
	if decided, ok := c.decided[request.Slot]; ok {
		needsSync := !c.durable[request.Slot]
		if !c.durable[request.Slot] {
			if !c.logged[request.Slot] {
				decision, err := decodeDecision(decided.Certificate)
				if err != nil {
					c.mu.Unlock()
					return Summary{}, err
				}
				if err := c.appendDecision(decision, decided.Value, decided.Certificate); err != nil {
					c.mu.Unlock()
					return Summary{}, err
				}
				c.logged[request.Slot] = true
			}
		}
		decision, err := decodeDecision(decided.Certificate)
		if err != nil {
			c.mu.Unlock()
			return Summary{}, err
		}
		decision.Proposal.Value = append([]byte(nil), decided.Value...)
		step := decision.Step
		if request.Step > step {
			step = request.Step
		}
		summary := Summary{
			RecorderID: c.nodeID, Step: step, FirstCurrent: cloneProposal(&decision.Proposal),
			AggregatePrior: cloneProposal(&decision.Proposal),
		}
		c.mu.Unlock()
		if needsSync {
			if err := c.commits.Sync(ctx); err != nil {
				return Summary{}, err
			}
			c.mu.Lock()
			c.durable[request.Slot] = true
			c.valueDurable[decided.Hash] = true
			c.mu.Unlock()
		}
		return summary, nil
	}
	state := c.recorders[request.Slot]
	known := sameProposal(state.FirstCurrent, &request.Proposal) ||
		sameProposal(state.AggregateCurrent, &request.Proposal) ||
		sameProposal(state.AggregatePrior, &request.Proposal)
	if value, ok := c.values[request.Proposal.Hash]; !ok && !known {
		c.mu.Unlock()
		return Summary{}, fmt.Errorf("proposal value is unavailable")
	} else if ok && len(request.Proposal.Value) != 0 && !bytes.Equal(request.Proposal.Value, value) {
		c.mu.Unlock()
		return Summary{}, fmt.Errorf("proposal hash mismatch")
	}
	request.Proposal.Value = nil
	epoch := leaderEpoch(request.Slot)
	if _, ok := c.epochStart[epoch]; !ok {
		c.epochStart[epoch] = c.now()
	}
	next, summary := state.Record(request.Step, request.Proposal)
	summary.RecorderID = c.nodeID
	payload := encodeRecorderEntry(request.Slot, next)
	if err := c.wal.Append(qlog.Entry{Slot: uint64(request.Slot), Hash: request.Proposal.Hash, Type: qlog.EntryReceipt, Payload: payload}); err != nil {
		c.mu.Unlock()
		return Summary{}, err
	}
	c.recorders[request.Slot] = next
	c.mu.Unlock()
	if err := c.commits.Sync(ctx); err != nil {
		return Summary{}, err
	}
	c.mu.Lock()
	c.valueDurable[request.Proposal.Hash] = true
	c.mu.Unlock()
	return summary, nil
}

func (c *Core) SetCheckpointValidator(validator func(context.Context, CheckpointSeal) error) {
	c.mu.Lock()
	c.checkpointValidator = validator
	c.mu.Unlock()
}

func (c *Core) validateCheckpointValue(ctx context.Context, hash ValueHash) error {
	value, ok := c.stagedValue(hash)
	if !ok {
		return nil
	}
	seal, checkpoint, err := DecodeCheckpointSeal(value)
	if err != nil || !checkpoint {
		return err
	}
	return c.RequirePreparedCheckpoint(seal)
}

func (c *Core) checkpointIdentity(seal CheckpointSeal) (bool, func(context.Context, CheckpointSeal) error, error) {
	c.mu.RLock()
	prefix, prefixOK := c.prefixes[seal.Index]
	preparedIndex, preparedRoot, prepared := c.latestPreparedCheckpointLocked()
	validator := c.checkpointValidator
	order, following, orderErr := c.checkpointLeaderOrdersLocked(seal.Index)
	tip := c.tip
	c.mu.RUnlock()
	if seal.ConfigID != c.config.ConfigID || !prefixOK || prefix != seal.PrefixHash || seal.Index > tip || orderErr != nil || !slices.Equal(order, seal.NextLeaderOrder) || !slices.Equal(following, seal.FollowingLeaderOrder) {
		return false, nil, fmt.Errorf("checkpoint seal does not match local certified prefix")
	}
	if prepared && seal.Index < preparedIndex {
		return false, nil, fmt.Errorf("checkpoint index %d is below prepared fence %d", seal.Index, preparedIndex)
	}
	if prepared && seal.Index == preparedIndex {
		if preparedRoot != seal.RootHash {
			return false, nil, fmt.Errorf("checkpoint index %d is already prepared with another root", seal.Index)
		}
		return true, validator, nil
	}
	return false, validator, nil
}

// RequirePreparedCheckpoint is the bounded Record-path check. Full object
// verification happens before consensus through PrepareCheckpoint.
func (c *Core) RequirePreparedCheckpoint(seal CheckpointSeal) error {
	verified, _, err := c.checkpointIdentity(seal)
	if err != nil {
		return err
	}
	if !verified {
		return fmt.Errorf("checkpoint root is not prepared")
	}
	return nil
}

// PrepareCheckpoint verifies a candidate outside Record RPC and persists the
// verified identity before this node may vote for its seal.
func (c *Core) PrepareCheckpoint(ctx context.Context, seal CheckpointSeal) error {
	c.checkpointMu.Lock()
	defer c.checkpointMu.Unlock()
	verified, validator, err := c.checkpointIdentity(seal)
	if err != nil {
		return err
	}
	if verified {
		return nil
	}
	if validator == nil {
		return fmt.Errorf("checkpoint validation is unavailable")
	}
	if err := validator(ctx, seal); err != nil {
		return err
	}
	payload, err := EncodeCheckpointSeal(seal)
	if err != nil {
		return err
	}
	if err := c.wal.Append(qlog.Entry{Slot: uint64(seal.Index), Hash: seal.RootHash, Type: qlog.EntryCheckpointVerified, Payload: payload}); err != nil {
		return err
	}
	if err := c.commits.Sync(ctx); err != nil {
		return err
	}
	c.mu.Lock()
	clear(c.preparedCheckpoints)
	c.preparedCheckpoints[seal.Index] = seal.RootHash
	c.mu.Unlock()
	return nil
}

// VerifyCheckpoint preserves the public API while using the prepare protocol.
func (c *Core) VerifyCheckpoint(ctx context.Context, seal CheckpointSeal) error {
	return c.PrepareCheckpoint(ctx, seal)
}

// AcceptDecision validates Algorithm 4 quorum evidence and records the decision durably.
func (c *Core) AcceptDecision(decision Decision) error {
	lock := &c.recordLocks[uint64(decision.Slot)%uint64(len(c.recordLocks))]
	lock.Lock()
	defer lock.Unlock()
	if err := c.acceptDecision(decision); err != nil {
		return err
	}
	return c.ensureDurableLocked(decision.Slot)
}

// AcceptDecisionHint installs a certified decision without a second disk
// barrier. The durable recorder quorum remains the recovery source; catch-up
// callers that require a local durable copy use AcceptDecision instead.
func (c *Core) AcceptDecisionHint(decision Decision) error {
	lock := &c.recordLocks[uint64(decision.Slot)%uint64(len(c.recordLocks))]
	lock.Lock()
	defer lock.Unlock()
	return c.acceptDecision(decision)
}

// EnsureDurable ensures the slot has a durable local decision marker or is
// already covered by the certified recovery base.
func (c *Core) EnsureDurable(slot Slot) error {
	lock := &c.recordLocks[uint64(slot)%uint64(len(c.recordLocks))]
	lock.Lock()
	defer lock.Unlock()
	return c.ensureDurableLocked(slot)
}

func (c *Core) ensureDurableLocked(slot Slot) error {
	c.mu.Lock()
	if slot == 0 {
		c.mu.Unlock()
		return fmt.Errorf("invalid QuePaxa slot")
	}
	if slot <= c.floor {
		c.mu.Unlock()
		return nil
	}
	decided, ok := c.decided[slot]
	if !ok {
		c.mu.Unlock()
		return fmt.Errorf("slot %d is not decided", slot)
	}
	if !c.logged[slot] {
		decision, err := decodeDecision(decided.Certificate)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		if err := c.appendDecision(decision, decided.Value, decided.Certificate); err != nil {
			c.mu.Unlock()
			return err
		}
		c.logged[slot] = true
	}
	if c.durable[slot] {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	if err := c.commits.Sync(context.Background()); err != nil {
		return err
	}
	c.mu.Lock()
	c.durable[slot] = true
	c.valueDurable[decided.Hash] = true
	c.mu.Unlock()
	return nil
}

// CompleteDecision makes an existing decision safe to acknowledge by
// re-establishing the learner quorum required by ReadIndex.
func (c *Core) CompleteDecision(ctx context.Context, slot Slot) (DecidedValue, error) {
	return c.completeDecision(ctx, slot, true)
}

func (c *Core) completeDecision(ctx context.Context, slot Slot, syncLocal bool) (DecidedValue, error) {
	if c.observer {
		return DecidedValue{}, ErrQuorumUnavailable
	}
	lock := &c.recordLocks[uint64(slot)%uint64(len(c.recordLocks))]
	lock.Lock()
	if syncLocal {
		if err := c.ensureDurableLocked(slot); err != nil {
			lock.Unlock()
			return DecidedValue{}, err
		}
	} else {
		c.mu.Lock()
		value, ok := c.decided[slot]
		if !ok {
			c.mu.Unlock()
			lock.Unlock()
			return DecidedValue{}, fmt.Errorf("slot %d is not decided", slot)
		}
		if !c.logged[slot] {
			decision, err := decodeDecision(value.Certificate)
			if err != nil {
				c.mu.Unlock()
				lock.Unlock()
				return DecidedValue{}, err
			}
			if err := c.appendDecision(decision, value.Value, value.Certificate); err != nil {
				c.mu.Unlock()
				lock.Unlock()
				return DecidedValue{}, err
			}
			c.logged[slot] = true
		}
		c.mu.Unlock()
	}
	c.mu.RLock()
	value, ok := c.decided[slot]
	compacted := slot <= c.floor
	value.Value = append([]byte(nil), value.Value...)
	value.Certificate = append([]byte(nil), value.Certificate...)
	c.mu.RUnlock()
	lock.Unlock()
	if compacted {
		return DecidedValue{}, fmt.Errorf("%w: slot %d is covered by the recovery floor", ErrCompacted, slot)
	}
	if !ok {
		return DecidedValue{}, fmt.Errorf("slot %d is not decided", slot)
	}
	decision, err := c.certifiedDecision(value)
	if err != nil {
		return DecidedValue{}, err
	}
	if err := c.WaitTip(ctx, slot); err != nil {
		return DecidedValue{}, err
	}
	if len(c.config.Members) <= 1 {
		return value, nil
	}
	if c.transport == nil {
		return DecidedValue{}, ErrQuorumUnavailable
	}
	if err := c.transport.SendDecision(ctx, decision); err != nil {
		return DecidedValue{}, fmt.Errorf("%w: learn decision: %v", ErrQuorumUnavailable, err)
	}
	return value, nil
}

func (c *Core) acceptDecision(decision Decision) error {
	if err := c.validateDecisionAtFloor(decision); err != nil {
		return err
	}
	certificate, err := encodeCertificate(c.config.ConfigID, decision)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if decision.Slot <= c.floor {
		c.mu.Unlock()
		return nil
	}
	if existing, ok := c.decided[decision.Slot]; ok {
		if existing.Hash != decision.Proposal.Hash || !bytes.Equal(existing.Value, decision.Proposal.Value) {
			c.mu.Unlock()
			return fmt.Errorf("slot %d already decided with another value", decision.Slot)
		}
		c.mu.Unlock()
		return nil
	}
	value := DecidedValue{Slot: decision.Slot, Hash: decision.Proposal.Hash, Value: append([]byte(nil), decision.Proposal.Value...), Certificate: certificate}
	c.values[decision.Proposal.Hash] = append([]byte(nil), decision.Proposal.Value...)
	if seal, checkpoint, _ := DecodeCheckpointSeal(decision.Proposal.Value); checkpoint {
		c.sealedRoots[seal.RootHash] = SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: decision.Slot}
	}
	c.decided[decision.Slot] = value
	c.durable[decision.Slot] = false
	c.logged[decision.Slot] = false
	c.updateHashIndexLocked(decision.Proposal.Hash, decision.Slot)
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

func (c *Core) appendDecision(decision Decision, value, certificate []byte) error {
	record, err := encodeDecisionRecord(value, certificate)
	if err != nil {
		return err
	}
	payload := append(append([]byte(nil), decisionEntryMagic...), record...)
	return c.wal.Append(qlog.Entry{Slot: uint64(decision.Slot), Hash: decision.Proposal.Hash, Type: qlog.EntryDecide, Payload: payload})
}

// RecorderTip returns the highest slot represented by recovered durable ISR
// state, including a decision whose certificate marker may have been lost.
func (c *Core) RecorderTip() Slot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var tip Slot
	for slot := range c.recorders {
		if slot > tip {
			tip = slot
		}
	}
	return tip
}

// RecoverThrough re-drives undecided slots from durable recorder state. It
// deliberately disables the leader fast path so a restarted leader cannot
// introduce a different highest-priority value for an old slot.
func (c *Core) RecoverThrough(ctx context.Context, through Slot) error {
	if c.observer {
		return ErrQuorumUnavailable
	}
	select {
	case c.pipeline <- struct{}{}:
		defer func() { <-c.pipeline }()
	case <-ctx.Done():
		return ctx.Err()
	}
	for c.Tip() < through {
		slot := c.Tip() + 1
		value, err := c.recoveryValue(ctx, slot)
		if err != nil {
			return fmt.Errorf("recover slot %d value: %w", slot, err)
		}
		decision, err := c.runSlot(ctx, slot, value, false)
		if err != nil {
			return fmt.Errorf("recover slot %d: %w", slot, err)
		}
		if err := c.AcceptDecision(decision); err != nil {
			return fmt.Errorf("persist recovered slot %d: %w", slot, err)
		}
	}
	return nil
}

func (c *Core) recoveryValue(ctx context.Context, slot Slot) ([]byte, error) {
	c.mu.RLock()
	state := c.recorders[slot]
	if proposal := maxProposal(maxProposal(state.FirstCurrent, state.AggregateCurrent), state.AggregatePrior); proposal != nil {
		value := append([]byte(nil), c.values[proposal.Hash]...)
		c.mu.RUnlock()
		if len(value) != 0 {
			return value, nil
		}
		sources := make([]NodeID, len(c.config.Members))
		for i := range c.config.Members {
			sources[i] = c.config.Members[i].ID
		}
		if err := c.hydrateProposal(ctx, proposal, sources...); err != nil {
			return nil, err
		}
		return proposal.Value, nil
	}
	c.mu.RUnlock()
	seed := sha256.Sum256([]byte(fmt.Sprintf("rhiza-recovery:%s:%d", c.nodeID, slot)))
	var nonce [ReadBarrierNonceSize]byte
	copy(nonce[:], seed[:])
	return EncodeReadBarrier(nonce), nil
}

func (c *Core) validateDecision(decision Decision) error {
	return c.validateDecisionForRecovery(decision, false)
}

func (c *Core) validateDecisionForRecovery(decision Decision, allowMissingLeader bool) error {
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
			epoch := leaderEpoch(decision.Slot)
			missingControl := epoch >= c.explorationEpochs() && !c.IsDecided(leaderEpochFirst(epoch-1))
			if !allowMissingLeader || !missingControl {
				return err
			}
		}
		if decision.Step != 4 || (len(order) > 0 && decision.Proposal.ProposerID != order[0]) || decision.Proposal.Priority != highestPriority {
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
	_, decision, err := decodeCertificate(certificate)
	return decision, err
}

// AcceptCertifiedValue binds catch-up metadata to its certificate before mutation.
func (c *Core) AcceptCertifiedValue(value DecidedValue) error {
	return c.AcceptCertifiedValues([]DecidedValue{value})
}

// AcceptCertifiedValueForAck installs a proposer-returned decision. A voter
// already named by its durable recorder certificate does not need a second
// local disk barrier; every other voter persists the decision before ACK.
func (c *Core) AcceptCertifiedValueForAck(value DecidedValue) error {
	decision, err := c.certifiedDecision(value)
	if err != nil {
		return err
	}
	for _, summary := range decision.Summaries {
		if summary.RecorderID == c.nodeID && len(c.config.Members) > 1 {
			c.mu.RLock()
			state := c.recorders[value.Slot]
			recorded := sameProposal(state.FirstCurrent, &decision.Proposal) ||
				sameProposal(state.AggregateCurrent, &decision.Proposal) ||
				sameProposal(state.AggregatePrior, &decision.Proposal)
			durable := c.valueDurable[value.Hash]
			c.mu.RUnlock()
			if recorded && durable {
				return c.AcceptCertifiedHints([]DecidedValue{value})
			}
		}
	}
	return c.AcceptCertifiedValue(value)
}

// AcceptCertifiedValues validates and persists a catch-up page with one sync.
func (c *Core) AcceptCertifiedValues(values []DecidedValue) error {
	return c.acceptCertifiedValues(values, true)
}

// AcceptCertifiedHints installs an on-demand page without adding a frontend
// disk barrier; the source recorder quorum remains durable.
func (c *Core) AcceptCertifiedHints(values []DecidedValue) error {
	return c.acceptCertifiedValues(values, false)
}

func (c *Core) acceptCertifiedValues(values []DecidedValue, durable bool) error {
	decisions := make([]Decision, len(values))
	for i, value := range values {
		decision, err := c.certifiedDecision(value)
		if err != nil {
			return err
		}
		decisions[i] = decision
	}
	unlock := c.lockDecisionSlots(values)
	defer unlock()
	c.mu.RLock()
	for _, decision := range decisions {
		if err := c.compactedErrorLocked(decision.Slot); err != nil {
			c.mu.RUnlock()
			return err
		}
	}
	c.mu.RUnlock()
	slots := make([]Slot, 0, len(values))
	for i, value := range values {
		decision := decisions[i]
		if err := c.acceptDecision(decision); err != nil {
			return err
		}
		if durable {
			c.mu.Lock()
			if err := c.compactedErrorLocked(decision.Slot); err != nil {
				c.mu.Unlock()
				return err
			}
			if !c.logged[decision.Slot] {
				if err := c.appendDecision(decision, value.Value, value.Certificate); err != nil {
					c.mu.Unlock()
					return err
				}
				c.logged[decision.Slot] = true
			}
			c.mu.Unlock()
		}
		slots = append(slots, decision.Slot)
	}
	if !durable || len(slots) == 0 {
		return nil
	}
	if err := c.commits.Sync(context.Background()); err != nil {
		return err
	}
	c.mu.Lock()
	for _, slot := range slots {
		c.durable[slot] = true
		if decided, ok := c.decided[slot]; ok {
			c.valueDurable[decided.Hash] = true
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Core) validateDecisionAtFloor(decision Decision) error {
	c.mu.RLock()
	covered := decision.Slot <= c.floor
	c.mu.RUnlock()
	return c.validateDecisionForRecovery(decision, covered)
}

func (c *Core) lockDecisionSlots(values []DecidedValue) func() {
	indexes := make([]int, 0, len(values))
	seen := make(map[int]struct{}, len(values))
	for _, value := range values {
		index := int(uint64(value.Slot) % uint64(len(c.recordLocks)))
		if _, ok := seen[index]; ok {
			continue
		}
		seen[index] = struct{}{}
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		c.recordLocks[index].Lock()
	}
	return func() {
		for i := len(indexes) - 1; i >= 0; i-- {
			c.recordLocks[indexes[i]].Unlock()
		}
	}
}

func (c *Core) rejectCompacted(slot Slot) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.compactedErrorLocked(slot)
}

func (c *Core) compactedErrorLocked(slot Slot) error {
	if slot <= c.floor {
		return fmt.Errorf("%w: slot %d is at or below floor %d", ErrCompacted, slot, c.floor)
	}
	return nil
}

func (c *Core) updateHashIndexLocked(hash ValueHash, slot Slot) {
	if current, ok := c.byHash[hash]; !ok || slot < current {
		c.byHash[hash] = slot
	}
}

func (c *Core) certifiedDecision(value DecidedValue) (Decision, error) {
	configID, decision, err := decodeCertificate(value.Certificate)
	if err != nil {
		return Decision{}, err
	}
	decision.Proposal.Value = append([]byte(nil), value.Value...)
	if configID != c.config.ConfigID || decision.Slot != value.Slot || decision.Proposal.Hash != value.Hash || sha256.Sum256(value.Value) != value.Hash {
		return Decision{}, fmt.Errorf("catch-up value does not match QuePaxa certificate")
	}
	return decision, nil
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
	if from <= c.floor {
		return nil, c.tip, fmt.Errorf("%w: requested slot %d is at or below floor %d", ErrCompacted, from, c.floor)
	}
	capacity := 0
	if from <= c.tip {
		capacity = limit
		if remaining := c.tip - from + 1; remaining < Slot(capacity) {
			capacity = int(remaining)
		}
	}
	page := make([]DecidedValue, 0, capacity)
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

func (c *Core) PrefixHash(slot Slot) ([32]byte, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	hash, ok := c.prefixes[slot]
	return hash, ok
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
	var prepared []CheckpointSeal
	if err := c.wal.Scan(func(entry qlog.Entry) error {
		switch entry.Type {
		case qlog.EntryCheckpoint:
			base, decodeErr := decodeConsensusBase(entry.Payload)
			if decodeErr != nil || base.ConfigID != c.config.ConfigID || uint64(base.ClosedThrough) != entry.Slot || base.RecoveryRoot != entry.Hash || base.LeaderEpoch != leaderEpoch(base.ClosedThrough+1) || !c.validateCheckpointLeaderOrders(base.ClosedThrough, base.NextLeaderOrder, base.FollowingLeaderOrder) {
				if decodeErr == nil {
					decodeErr = fmt.Errorf("consensus base identity mismatch")
				}
				return fmt.Errorf("recover consensus base: %w", decodeErr)
			}
			c.mu.Lock()
			c.installBaseLocked(base)
			c.mu.Unlock()
		case qlog.EntryCheckpointVerified:
			seal, checkpoint, decodeErr := DecodeCheckpointSeal(entry.Payload)
			if decodeErr != nil || !checkpoint || uint64(seal.Index) != entry.Slot || seal.RootHash != entry.Hash || seal.ConfigID != c.config.ConfigID || !c.validateCheckpointLeaderOrders(seal.Index, seal.NextLeaderOrder, seal.FollowingLeaderOrder) {
				if decodeErr == nil {
					decodeErr = fmt.Errorf("checkpoint verification identity mismatch")
				}
				return fmt.Errorf("recover verified checkpoint: %w", decodeErr)
			}
			prepared = append(prepared, seal)
		case qlog.EntryProposal:
			if len(entry.Payload) == 0 || sha256.Sum256(entry.Payload) != entry.Hash {
				return fmt.Errorf("recover QuePaxa value identity mismatch")
			}
			c.values[entry.Hash] = entry.Payload
			c.valueDurable[entry.Hash] = true
		case qlog.EntryReceipt:
			persisted, err := decodeRecorderEntry(entry.Payload)
			if err != nil {
				return fmt.Errorf("recover QuePaxa ISR: %w", err)
			}
			c.mu.Lock()
			if _, decided := c.decided[persisted.Slot]; !decided {
				c.recorders[persisted.Slot] = persisted.State
			}
			c.mu.Unlock()
		case qlog.EntryDecide:
			if !bytes.HasPrefix(entry.Payload, decisionEntryMagic) {
				return fmt.Errorf("recover QuePaxa decision: unknown WAL record format")
			}
			value, certificate, err := decodeDecisionRecord(entry.Payload[len(decisionEntryMagic):])
			if err != nil {
				return err
			}
			configID, decision, err := decodeCertificate(certificate)
			if err != nil {
				return err
			}
			if configID != c.config.ConfigID || sha256.Sum256(value) != decision.Proposal.Hash {
				return fmt.Errorf("recover QuePaxa decision identity mismatch")
			}
			decision.Proposal.Value = value
			if err := c.validateDecisionForRecovery(decision, true); err != nil {
				return fmt.Errorf("recover QuePaxa decision: %w", err)
			}
			c.mu.Lock()
			if existing, ok := c.decided[decision.Slot]; ok && existing.Hash != decision.Proposal.Hash {
				c.mu.Unlock()
				return fmt.Errorf("conflicting decisions at slot %d", decision.Slot)
			}
			c.decided[decision.Slot] = DecidedValue{Slot: decision.Slot, Hash: decision.Proposal.Hash, Value: value, Certificate: certificate}
			c.values[decision.Proposal.Hash] = value
			c.valueDurable[decision.Proposal.Hash] = true
			if seal, checkpoint, _ := DecodeCheckpointSeal(value); checkpoint {
				c.sealedRoots[seal.RootHash] = SealedCheckpoint{CheckpointSeal: seal, DecisionSlot: decision.Slot}
			}
			c.durable[decision.Slot] = true
			c.logged[decision.Slot] = true
			c.updateHashIndexLocked(decision.Proposal.Hash, decision.Slot)
			delete(c.recorders, decision.Slot)
			c.mu.Unlock()
		}
		return nil
	}); err != nil {
		return err
	}
	c.mu.RLock()
	recovered := make([]Decision, 0, len(c.decided))
	for _, value := range c.decided {
		decision, err := decodeDecision(value.Certificate)
		if err != nil {
			c.mu.RUnlock()
			return err
		}
		decision.Proposal.Value = value.Value
		recovered = append(recovered, decision)
	}
	c.mu.RUnlock()
	sort.Slice(recovered, func(i, j int) bool { return recovered[i].Slot < recovered[j].Slot })
	for _, decision := range recovered {
		if err := c.validateDecisionForRecovery(decision, true); err != nil {
			return fmt.Errorf("recover QuePaxa decision: %w", err)
		}
	}
	c.mu.Lock()
	c.advanceTipLocked()
	c.mu.Unlock()
	for _, seal := range prepared {
		if _, _, err := c.checkpointIdentity(seal); err != nil {
			return fmt.Errorf("recover verified checkpoint: %w", err)
		}
		c.mu.Lock()
		if root, exists := c.preparedCheckpoints[seal.Index]; exists && root != seal.RootHash {
			c.mu.Unlock()
			return fmt.Errorf("recover verified checkpoint: conflicting roots at index %d", seal.Index)
		}
		c.preparedCheckpoints[seal.Index] = seal.RootHash
		c.mu.Unlock()
	}
	return nil
}

func (c *Core) advanceTipLocked() {
	before := c.tip
	for {
		decision, ok := c.decided[c.tip+1]
		if !ok {
			break
		}
		c.tip++
		c.prefixes[c.tip] = AdvancePrefixHash(c.prefixes[c.tip-1], c.tip, decision.Hash)
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
	c.periodicMu.Lock()
	if c.periodic != nil {
		c.periodic()
		c.periodicWG.Wait()
	}
	ctx, c.periodic = context.WithCancel(ctx)
	c.periodicWG.Add(1)
	c.periodicMu.Unlock()
	go func() {
		defer c.periodicWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := c.wal.Sync()
				c.healthMu.Lock()
				c.periodicErr = err
				c.healthMu.Unlock()
			}
		}
	}()
}

// Health reports background durability failures that would otherwise be
// invisible until shutdown.
func (c *Core) Health() error {
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	return c.periodicErr
}

// StopPeriodicSync waits until the background loop no longer uses the WAL.
func (c *Core) StopPeriodicSync() {
	c.periodicMu.Lock()
	if c.periodic != nil {
		c.periodic()
		c.periodic = nil
	}
	c.periodicMu.Unlock()
	c.periodicWG.Wait()
}
