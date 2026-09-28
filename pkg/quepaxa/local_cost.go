package quepaxa

import (
	"context"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/mrchypark/rhiza/pkg/qlog"
)

const localValueLimit = uint64(MaxReplicatedValueBytes)
const localQLogEntryHeaderBytes = 53

var errLocalCostsUnavailable = errors.New("Local WAL costs require explicit local mode")

// localAdmissionDenial is returned only by a top-level Local preflight before
// that operation has changed Core or WAL state. It is not a generic capacity
// error and must not be used to classify failures from nested callbacks.
type localAdmissionDenial struct {
	operation string
	reason    string
	used      uint64
	limit     uint64
	immediate uint64
	protected uint64
}

func (e *localAdmissionDenial) Error() string {
	return fmt.Sprintf("Local WAL admission denied: operation=%s reason=%s used=%d immediate=%d protected=%d limit=%d", e.operation, e.reason, e.used, e.immediate, e.protected, e.limit)
}

// IsLocalAdmissionDenial recognizes Core's dedicated pre-write refusal. The
// unexported concrete type prevents callers from manufacturing the exemption.
func IsLocalAdmissionDenial(err error) bool {
	var denial *localAdmissionDenial
	return errors.As(err, &denial)
}

// LocalWALCost is a read-only encoded-byte estimate. SlotBytes bounds one
// supported slot attempt. ForegroundBytes bounds one offered-value slot plus
// one maximal normalized-prefix/schedule slot. RecoveryBytes bounds one
// maximal slot of RecoverThrough work; neither covers an arbitrary multi-slot
// pending prefix. These
// costs do not reserve WAL space or perform I/O.
type LocalWALCost struct {
	ProposalEntryBytes uint64
	ReceiptEntryBytes  uint64
	DecisionEntryBytes uint64
	SlotBytes          uint64
	ForegroundBytes    uint64
	RecoveryBytes      uint64
	CheckpointBytes    uint64
}

type localCostModel struct {
	ready                  bool
	nodeID                 NodeID
	configID               uint
	valueLimit             uint64
	entryHeaderBytes       uint64
	receiptEntryBytes      uint64
	certificateBytes       uint64
	checkpointSealBytes    uint64
	leaderScheduleBytes    uint64
	maxSlotBytes           uint64
	maxRecoveryBytes       uint64
	maxCheckpointCostBytes uint64
}

func newLocalCostModel(nodeID NodeID, configID uint) (localCostModel, error) {
	if !utf8.ValidString(string(nodeID)) {
		return localCostModel{}, fmt.Errorf("%w: Local node ID must be valid UTF-8", ErrInvalidConfig)
	}
	if nodeID == "" {
		return localCostModel{}, fmt.Errorf("%w: Local node ID is required", ErrInvalidConfig)
	}

	var maxHash [32]byte
	for i := range maxHash {
		maxHash[i] = 0xff
	}
	baseSeal := CheckpointSeal{
		ConfigID: configID, Index: Slot(math.MaxUint64),
		RootHash: maxHash, StateHash: maxHash, PrefixHash: maxHash,
		NextLeaderOrder: []NodeID{""}, FollowingLeaderOrder: []NodeID{""},
		GenerationAnchorHash: maxHash,
	}
	minimumSeal, err := EncodeCheckpointSeal(baseSeal)
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: encode Local checkpoint seal minimum: %v", ErrInvalidConfig, err)
	}
	if uint64(len(minimumSeal)) > localValueLimit {
		return localCostModel{}, fmt.Errorf("%w: minimum Local checkpoint seal exceeds value limit", ErrInvalidConfig)
	}
	quotedIDBytes, err := jsonQuotedStringLength(string(nodeID))
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: size Local node ID: %v", ErrInvalidConfig, err)
	}
	idGrowth := quotedIDBytes - 2 // the empty-string template already has quotes.
	minimumSchedule, err := EncodeLeaderSchedule([]NodeID{""})
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: encode Local leader schedule minimum: %v", ErrInvalidConfig, err)
	}
	projectedScheduleBytes, err := checkedAdd(uint64(len(minimumSchedule)), idGrowth)
	if err != nil || projectedScheduleBytes > localValueLimit {
		return localCostModel{}, fmt.Errorf("%w: Local node ID cannot fit the bounded leader schedule", ErrInvalidConfig)
	}
	projectedSealBytes, err := checkedAdd(uint64(len(minimumSeal)), idGrowth, idGrowth)
	if err != nil || projectedSealBytes > localValueLimit {
		return localCostModel{}, fmt.Errorf("%w: Local node ID cannot fit the bounded checkpoint seal", ErrInvalidConfig)
	}

	model := localCostModel{
		ready:            true,
		nodeID:           nodeID,
		configID:         configID,
		valueLimit:       localValueLimit,
		entryHeaderBytes: uint64(len((qlog.Entry{}).Encode())),
	}
	if model.entryHeaderBytes == 0 {
		return localCostModel{}, fmt.Errorf("%w: empty qlog entry header", ErrInvalidConfig)
	}
	if model.entryHeaderBytes != localQLogEntryHeaderBytes {
		return localCostModel{}, fmt.Errorf("%w: qlog entry header size changed: got %d want %d", ErrInvalidConfig, model.entryHeaderBytes, localQLogEntryHeaderBytes)
	}
	orderValue, err := EncodeLeaderSchedule([]NodeID{nodeID})
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: encode Local leader schedule bound: %v", ErrInvalidConfig, err)
	}
	if uint64(len(orderValue)) > localValueLimit {
		return localCostModel{}, fmt.Errorf("%w: Local leader schedule exceeds value limit", ErrInvalidConfig)
	}
	if uint64(len(orderValue)) != projectedScheduleBytes {
		return localCostModel{}, fmt.Errorf("%w: Local leader schedule size disagrees with JSON bound", ErrInvalidConfig)
	}
	model.leaderScheduleBytes = uint64(len(orderValue))

	seal := baseSeal
	seal.NextLeaderOrder = []NodeID{nodeID}
	seal.FollowingLeaderOrder = []NodeID{nodeID}
	sealBytes, err := EncodeCheckpointSeal(seal)
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: encode Local checkpoint seal bound: %v", ErrInvalidConfig, err)
	}
	if uint64(len(sealBytes)) > localValueLimit {
		return localCostModel{}, fmt.Errorf("%w: Local checkpoint seal exceeds value limit", ErrInvalidConfig)
	}
	if uint64(len(sealBytes)) != projectedSealBytes {
		return localCostModel{}, fmt.Errorf("%w: Local checkpoint seal size disagrees with JSON bound", ErrInvalidConfig)
	}
	model.checkpointSealBytes = uint64(len(sealBytes))

	model.receiptEntryBytes, err = localReceiptEntryBound(nodeID, model.entryHeaderBytes)
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: encode Local receipt bound: %v", ErrInvalidConfig, err)
	}
	model.certificateBytes, err = localCertificateBound(nodeID, configID)
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: encode Local certificate bound: %v", ErrInvalidConfig, err)
	}
	model.maxSlotBytes, err = model.slotBytes(localValueLimit)
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: calculate Local slot bound: %v", ErrInvalidConfig, err)
	}
	model.maxRecoveryBytes = model.maxSlotBytes
	checkpointProposalBytes, err := model.entryBytes(localValueLimit)
	if err != nil {
		return localCostModel{}, err
	}
	doubleSlot, err := checkedMul(model.maxSlotBytes, 2)
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: calculate Local checkpoint bound: %v", ErrInvalidConfig, err)
	}
	model.maxCheckpointCostBytes, err = checkedAdd(checkpointProposalBytes, doubleSlot)
	if err != nil {
		return localCostModel{}, fmt.Errorf("%w: calculate Local checkpoint bound: %v", ErrInvalidConfig, err)
	}
	return model, nil
}

func jsonQuotedStringLength(value string) (uint64, error) {
	length := uint64(2) // opening and closing quotes
	for _, char := range value {
		var encoded uint64
		switch char {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			encoded = 2
		case '<', '>', '&', '\u2028', '\u2029':
			encoded = 6
		default:
			if char < 0x20 {
				encoded = 6
			} else {
				runeBytes := utf8.RuneLen(char)
				if runeBytes < 0 {
					return 0, errors.New("invalid UTF-8")
				}
				encoded = uint64(runeBytes)
			}
		}
		var err error
		length, err = checkedAdd(length, encoded)
		if err != nil {
			return 0, err
		}
	}
	return length, nil
}

func localReceiptEntryBound(nodeID NodeID, headerBytes uint64) (uint64, error) {
	var maxPriority Priority
	var maxHash ValueHash
	for i := range maxPriority {
		maxPriority[i] = 0xff
		maxHash[i] = 0xff
	}
	lowPriority := maxPriority
	lowPriority[len(lowPriority)-1]--
	lower := &Proposal{Priority: lowPriority, ProposerID: nodeID, Hash: maxHash}
	higher := &Proposal{Priority: maxPriority, ProposerID: nodeID, Hash: maxHash}
	states := [...]ISR{
		{Step: 4, FirstCurrent: lower, AggregateCurrent: lower},
		{Step: 4, FirstCurrent: lower, AggregateCurrent: higher},
		{Step: 5, FirstCurrent: lower, AggregateCurrent: lower, AggregatePrior: lower},
		{Step: 5, FirstCurrent: lower, AggregateCurrent: lower, AggregatePrior: higher},
		{Step: 6, FirstCurrent: higher, AggregateCurrent: higher, AggregatePrior: higher},
	}
	var maximum uint64
	for _, state := range states {
		payload := encodeRecorderEntry(1, state, false)
		entryBytes, err := checkedAdd(headerBytes, uint64(len(payload)))
		if err != nil {
			return 0, err
		}
		if entryBytes > maximum {
			maximum = entryBytes
		}
	}
	return maximum, nil
}

func localCertificateBound(nodeID NodeID, configID uint) (uint64, error) {
	// The all-0xff vectors maximize JSON numeric width. This is a structural
	// codec ceiling, not a claim that the synthetic hash matches a real value.
	var priority Priority
	var hash ValueHash
	for i := range priority {
		priority[i] = 0xff
		hash[i] = 0xff
	}
	proposal := Proposal{Priority: priority, ProposerID: nodeID, Hash: hash}
	decision := Decision{
		Slot: Slot(math.MaxUint64), Step: 6, Proposal: proposal,
		Summaries: []Summary{{
			RecorderID: nodeID, Step: 6,
			FirstCurrent: &proposal, AggregatePrior: &proposal,
		}},
	}
	certificate, err := encodeCertificate(configID, decision)
	if err != nil {
		return 0, err
	}
	return uint64(len(certificate)), nil
}

func (m localCostModel) entryBytes(valueBytes uint64) (uint64, error) {
	return checkedAdd(m.entryHeaderBytes, valueBytes)
}

func (m localCostModel) decisionBytes(valueBytes uint64) (uint64, error) {
	encodedValueBytes, err := base64Length(valueBytes)
	if err != nil {
		return 0, err
	}
	payloadBytes, err := checkedAdd(27, encodedValueBytes)
	if err != nil {
		return 0, err
	}
	// A zero-length non-nil []byte marshals as "", but nil []byte marshals
	// as null. Length-only callers cannot distinguish them, so cover null.
	if valueBytes == 0 {
		payloadBytes, err = checkedAdd(payloadBytes, 2)
		if err != nil {
			return 0, err
		}
	}
	payloadBytes, err = checkedAdd(payloadBytes, m.certificateBytes)
	if err != nil {
		return 0, err
	}
	payloadBytes, err = checkedAdd(payloadBytes, uint64(len(decisionEntryMagic)))
	if err != nil {
		return 0, err
	}
	return checkedAdd(m.entryHeaderBytes, payloadBytes)
}

func (m localCostModel) slotBytes(valueBytes uint64) (uint64, error) {
	proposalBytes, err := m.entryBytes(valueBytes)
	if err != nil {
		return 0, err
	}
	receipts, err := checkedMul(m.receiptEntryBytes, 3)
	if err != nil {
		return 0, err
	}
	decisionBytes, err := m.decisionBytes(valueBytes)
	if err != nil {
		return 0, err
	}
	return checkedAdd(proposalBytes, receipts, decisionBytes)
}

func (m localCostModel) estimate(valueBytes int) (LocalWALCost, error) {
	if !m.ready {
		return LocalWALCost{}, errLocalCostsUnavailable
	}
	if valueBytes < 0 || uint64(valueBytes) > m.valueLimit {
		return LocalWALCost{}, fmt.Errorf("Local value length %d exceeds limit %d", valueBytes, m.valueLimit)
	}
	valueSize := uint64(valueBytes)
	proposal, err := m.entryBytes(valueSize)
	if err != nil {
		return LocalWALCost{}, err
	}
	decision, err := m.decisionBytes(valueSize)
	if err != nil {
		return LocalWALCost{}, err
	}
	slot, err := m.slotBytes(valueSize)
	if err != nil {
		return LocalWALCost{}, err
	}
	foreground, err := checkedAdd(slot, m.maxSlotBytes)
	if err != nil {
		return LocalWALCost{}, err
	}
	return LocalWALCost{
		ProposalEntryBytes: proposal,
		ReceiptEntryBytes:  m.receiptEntryBytes,
		DecisionEntryBytes: decision,
		SlotBytes:          slot,
		ForegroundBytes:    foreground,
		RecoveryBytes:      m.maxRecoveryBytes,
		CheckpointBytes:    m.maxCheckpointCostBytes,
	}, nil
}

// LocalWALCosts returns checked per-Core estimates without changing Core or WAL
// state. The estimate is specific to this Core's immutable Local identity.
func (c *Core) LocalWALCosts(valueBytes int) (LocalWALCost, error) {
	if !c.localMode {
		return LocalWALCost{}, errLocalCostsUnavailable
	}
	return c.localCost.estimate(valueBytes)
}

// ValidateLocalValueCost validates only replicated-value length and cost
// arithmetic. It does not normalize, copy, or append the supplied bytes.
func (c *Core) ValidateLocalValueCost(value []byte) error {
	if !c.localMode {
		return errLocalCostsUnavailable
	}
	_, err := c.localCost.estimate(len(value))
	return err
}

// LocalUndurableDecisionBytes reports exact encoded QDEC bytes the retained
// suffix would append through without modifying the WAL. Decisions at or below
// the recovery floor cost nothing; already-logged decisions need only Sync.
func (c *Core) LocalUndurableDecisionBytes(ctx context.Context, through Slot) (uint64, error) {
	if !c.localMode {
		return 0, errLocalCostsUnavailable
	}
	if err := c.acquireLocalExecution(ctx); err != nil {
		return 0, err
	}
	defer c.releaseLocalExecution()
	return c.localUndurableDecisionBytesOwned(through)
}

func (c *Core) localUndurableDecisionBytesOwned(through Slot) (uint64, error) {
	if !c.localMode || len(c.localExecutionOwner) == 0 {
		return 0, errLocalExternalMutation
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if through <= c.floor {
		return 0, nil
	}
	var total uint64
	for slot := c.floor + 1; ; slot++ {
		decision, ok := c.decided[slot]
		if !ok {
			return 0, fmt.Errorf("cannot estimate Local recovery through slot %d: missing decided slot %d", through, slot)
		}
		if !c.logged[slot] {
			entry, err := decisionEntry(decision)
			if err != nil {
				return 0, fmt.Errorf("estimate Local decision slot %d: %w", slot, err)
			}
			total, err = checkedAdd(total, uint64(len(entry.Encode())))
			if err != nil {
				return 0, fmt.Errorf("Local recovery byte count overflows: %w", err)
			}
		}
		if slot == through {
			break
		}
	}
	return total, nil
}

// localRecoveryBytesOwned plans the whole retained suffix without allocating
// or iterating by slot. Exact QDEC bytes are counted for retained undecided
// history markers; undecided holes use the proven maximum supported slot cost.
func (c *Core) localRecoveryBytesOwned(through Slot, operation string, immediate, protected uint64) (uint64, error) {
	if !c.localMode || len(c.localExecutionOwner) == 0 {
		return 0, errLocalExternalMutation
	}
	c.mu.RLock()
	contiguous := c.tip
	hasUnlogged := c.localUnloggedCount != 0
	if through <= c.tip && !hasUnlogged {
		c.mu.RUnlock()
		return 0, nil
	}
	c.mu.RUnlock()
	rangeCount := uint64(0)
	if through > contiguous {
		rangeCount = uint64(through - contiguous)
	}
	decidedCount := uint64(0)
	if rangeCount != 0 {
		c.mu.RLock()
		for slot := range c.decided {
			if slot > contiguous && slot <= through {
				decidedCount++
			}
		}
		c.mu.RUnlock()
	}
	if decidedCount > rangeCount {
		return 0, fmt.Errorf("Local recovery decision count exceeds suffix length")
	}
	undecided := rangeCount - decidedCount
	lowerBound, err := checkedMul(undecided, c.localCost.maxSlotBytes)
	if err != nil {
		return 0, fmt.Errorf("Local recovery byte count overflows: %w", err)
	}
	lowerProtected, err := checkedAdd(protected, lowerBound)
	if err != nil {
		return 0, err
	}
	if err := c.localCapacityAdmissionOwned(operation, "protected_reserve", immediate, lowerProtected); err != nil {
		return 0, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var exact uint64
	for slot, decision := range c.decided {
		if slot > through || slot <= c.floor {
			continue
		}
		if !c.logged[slot] {
			entry, err := decisionEntry(decision)
			if err != nil {
				return 0, fmt.Errorf("estimate Local decision slot %d: %w", slot, err)
			}
			exact, err = checkedAdd(exact, uint64(len(entry.Encode())))
			if err != nil {
				return 0, fmt.Errorf("Local recovery byte count overflows: %w", err)
			}
		}
	}
	for slot, state := range c.recorders {
		if slot <= c.tip || slot > through {
			continue
		}
		if _, decided := c.decided[slot]; decided {
			continue
		}
		if err := c.validateLocalISRLocked(slot, state, nil); err != nil {
			return 0, err
		}
	}
	unknown, err := checkedMul(undecided, c.localCost.maxSlotBytes)
	if err != nil {
		return 0, fmt.Errorf("Local recovery byte count overflows: %w", err)
	}
	return checkedAdd(exact, unknown)
}

func (c *Core) localCapacityAdmissionOwned(operation, reason string, immediate, protected uint64) error {
	if !c.localMode || len(c.localExecutionOwner) == 0 {
		return errLocalExternalMutation
	}
	snapshot := c.wal.Capacity()
	if snapshot.Fatal != nil {
		return snapshot.Fatal
	}
	if snapshot.Limit <= 0 || snapshot.Used < 0 {
		return fmt.Errorf("Local WAL requires a finite positive capacity limit")
	}
	used, limit := uint64(snapshot.Used), uint64(snapshot.Limit)
	needed, err := checkedAdd(used, immediate, protected)
	if err != nil || needed > limit {
		return &localAdmissionDenial{operation: operation, reason: reason, used: used, limit: limit, immediate: immediate, protected: protected}
	}
	return nil
}

func (c *Core) localMissingDecisionBytesOwned(first, through Slot) (uint64, error) {
	if !c.localMode || len(c.localExecutionOwner) == 0 {
		return 0, errLocalExternalMutation
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.localUnloggedCount == 0 || through < first {
		return 0, nil
	}
	var total uint64
	for slot, decision := range c.decided {
		if slot < first || slot > through || slot <= c.floor || c.logged[slot] {
			continue
		}
		entry, err := decisionEntry(decision)
		if err != nil {
			return 0, fmt.Errorf("estimate Local decision slot %d: %w", slot, err)
		}
		total, err = checkedAdd(total, uint64(len(entry.Encode())))
		if err != nil {
			return 0, fmt.Errorf("Local decision byte count overflows: %w", err)
		}
	}
	return total, nil
}

func (c *Core) localValidateDecisionPrefixOwned(through Slot) error {
	if !c.localMode || len(c.localExecutionOwner) == 0 {
		return errLocalExternalMutation
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if through <= c.floor {
		return nil
	}
	rangeCount := uint64(through - c.floor)
	var found uint64
	for slot := range c.decided {
		if slot > c.floor && slot <= through {
			found++
		}
	}
	if found != rangeCount {
		return fmt.Errorf("Local decision prefix through slot %d contains gaps", through)
	}
	return nil
}

func decisionEntry(value DecidedValue) (qlog.Entry, error) {
	record, err := encodeDecisionRecord(value.Value, value.Certificate)
	if err != nil {
		return qlog.Entry{}, err
	}
	payload := append(append([]byte(nil), decisionEntryMagic...), record...)
	return qlog.Entry{Slot: uint64(value.Slot), Hash: value.Hash, Type: qlog.EntryDecide, Payload: payload}, nil
}

func base64Length(valueBytes uint64) (uint64, error) {
	groups := valueBytes / 3
	if valueBytes%3 != 0 {
		groups++
	}
	return checkedMul(groups, 4)
}

func checkedAdd(values ...uint64) (uint64, error) {
	var total uint64
	for _, value := range values {
		if math.MaxUint64-total < value {
			return 0, errors.New("byte count overflows uint64")
		}
		total += value
	}
	return total, nil
}

func checkedMul(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, errors.New("byte count overflows uint64")
	}
	return left * right, nil
}
