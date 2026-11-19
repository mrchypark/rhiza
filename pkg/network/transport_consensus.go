package network

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/network/peerfb"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func (t *Transport) FetchDecisions(ctx context.Context, source quepaxa.NodeID, from quepaxa.Slot, limit int) (result DecisionsResponse, resultErr error) {
	defer func() {
		if resultErr != nil {
			resultErr = fmt.Errorf("peer operation=sync source_hash=%x: %w", sha256.Sum256([]byte(source)), resultErr)
		}
	}()
	// Sync is a read-only transfer. Route to a current voter even when the
	// requested page begins before that voter was admitted; Core validates every
	// returned certificate against the configuration for its own slot.
	peer, err := t.transportForCurrent()
	if err != nil {
		return DecisionsResponse{}, err
	}
	if peer != t {
		return peer.FetchDecisions(ctx, source, from, limit)
	}
	req := t.request(peerfb.OperationSync)
	req.From, req.Limit = uint64(from), uint32(limit)
	response, err := t.call(ctx, source, req)
	if err != nil {
		return DecisionsResponse{}, err
	}
	result = DecisionsResponse{ClusterID: types.ClusterID(response.ClusterId), ProposerID: quepaxa.NodeID(response.ProposerId), ConfigID: uint(response.ConfigId), Tip: quepaxa.Slot(response.Tip), Decisions: make([]quepaxa.DecidedValue, len(response.Decisions))}
	for i := range response.Decisions {
		result.Decisions[i], err = decidedFromWire(response.Decisions[i])
		if err != nil {
			return DecisionsResponse{}, err
		}
	}
	if result.ClusterID != t.clusterID || result.ProposerID != source || result.ConfigID != t.configID {
		return DecisionsResponse{}, fmt.Errorf("catch-up source identity mismatch")
	}
	return result, nil
}

// FetchFrozenTargetDecisions routes a certified Sync request through the exact
// frozen target. The server still authenticates the stale retained sender and
// the local Core validates every returned decision and terminal certificate.
func (t *Transport) FetchFrozenTargetDecisions(ctx context.Context, source quepaxa.NodeID, from quepaxa.Slot, limit int, target quepaxa.Cluster) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	if _, ok := target.MemberSet()[source]; !ok {
		return nil, 0, fmt.Errorf("frozen target source is not a retained member")
	}
	peer, err := t.transportFor(target)
	if err != nil {
		return nil, 0, err
	}
	result, err := peer.FetchDecisions(ctx, source, from, limit)
	if err != nil {
		return nil, 0, err
	}
	if result.ClusterID != t.clusterID || result.ProposerID != source || result.ConfigID != target.ConfigID {
		return nil, 0, fmt.Errorf("frozen target recovery source identity mismatch")
	}
	return result.Decisions, result.Tip, nil
}

func (t *Transport) SendRecord(ctx context.Context, to quepaxa.NodeID, request quepaxa.RecordRequest) (quepaxa.Summary, error) {
	peer, closePeer, err := t.transportForSlotLease(request.Slot)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	if closePeer != nil {
		defer closePeer()
	}
	if peer != t {
		return peer.SendRecord(ctx, to, request)
	}
	if request.ConfigID != 0 && request.ConfigID != t.configID {
		return quepaxa.Summary{}, fmt.Errorf("record configuration mismatch")
	}
	req := t.request(peerfb.OperationRecord)
	req.Record = &peerfb.RecordRequestT{Slot: uint64(request.Slot), Step: uint64(request.Step), Proposal: proposalToWire(request.Proposal)}
	if request.ReconfigurationID != (quepaxa.ValueHash{}) {
		req.Hash = append([]byte(nil), request.ReconfigurationID[:]...)
	}
	response, err := t.callQuorum(ctx, to, req)
	if err != nil {
		return quepaxa.Summary{}, err
	}
	summary, err := summaryFromWire(response.Summary)
	if err == nil && summary.RecorderID != to {
		err = fmt.Errorf("recorder identity mismatch: want %s got %s", to, summary.RecorderID)
	}
	return summary, err
}

func (t *Transport) SendDecision(ctx context.Context, decision quepaxa.Decision) error {
	if core := t.resolverSnapshot(); core != nil {
		old, next := core.ClusterForSlot(decision.Slot), core.ClusterForSlot(decision.Slot+1)
		if old.ConfigID != next.ConfigID {
			return t.sendTerminalDecision(ctx, decision, old, next)
		}
		if control, err := quepaxa.DecodeReconfiguration(decision.Proposal.Value); err != nil {
			return err
		} else if control {
			peer, closePeer, err := t.transportForSlotLease(decision.Slot)
			if err != nil {
				return err
			}
			if closePeer != nil {
				defer closePeer()
			}
			return peer.sendFreezeDecision(ctx, decision)
		}
	}
	peer, closePeer, err := t.transportForSlotLease(decision.Slot)
	if err != nil {
		return err
	}
	if closePeer != nil {
		defer closePeer()
	}
	if peer != t {
		return peer.SendDecision(ctx, decision)
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type learnerDiagnosticResult struct {
		id  quepaxa.NodeID
		err error
	}
	diagnosticKind := ""
	var diagnosticResults *[2]learnerDiagnosticResult
	if localtesthooks.Enabled && len(t.members) == 3 {
		diagnosticKind = foregroundLearnDiagnosticKind(decision.Proposal.Value)
		if diagnosticKind != "" {
			diagnosticResults = new([2]learnerDiagnosticResult)
		}
	}
	results := make(chan error, len(t.members)-1)
	pending := 0
	if diagnosticResults == nil {
		for _, member := range t.members {
			if member.ID == t.localID {
				continue
			}
			pending++
			go func(member quepaxa.Member) {
				req := t.request(peerfb.OperationLearned)
				req.Decision = decisionToWire(decision)
				_, err := t.callQuorum(callCtx, member.ID, req)
				results <- err
			}(member)
		}
	} else {
		peerIndex := 0
		for _, member := range t.members {
			if member.ID == t.localID {
				continue
			}
			pending++
			index := peerIndex
			peerIndex++
			go func(member quepaxa.Member, index int) {
				req := t.request(peerfb.OperationLearned)
				req.Decision = decisionToWire(decision)
				_, err := t.callQuorum(callCtx, member.ID, req)
				diagnosticResults[index] = learnerDiagnosticResult{id: member.ID, err: err}
				results <- err
			}(member, index)
		}
	}
	successes := 1 // local learner
	var firstErr error
	for range pending {
		err := <-results
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
		} else {
			successes++
			if successes >= len(t.members)/2+1 {
				return nil
			}
		}
	}
	if firstErr != nil {
		if diagnosticKind != "" {
			contextState := "active"
			if ctx.Err() != nil {
				contextState = foregroundLearnDiagnosticErrorCategory(ctx.Err())
			}
			orderedResults := diagnosticResults[:]
			sort.Slice(orderedResults, func(i, j int) bool { return orderedResults[i].id < orderedResults[j].id })
			peerStates := make([]string, 0, len(orderedResults))
			for _, result := range orderedResults {
				category := "ack"
				if result.err != nil {
					category = foregroundLearnDiagnosticErrorCategory(result.err)
				}
				peerStates = append(peerStates, fmt.Sprintf("%s=%s", result.id, category))
			}
			phase := "checkpoint-seal"
			if diagnosticKind == "read-barrier" {
				phase = "read-barrier-advance"
			}
			localtesthooks.Hit(fmt.Sprintf("network:foreground-learn-failure:node=%s:kind=%s:phase=%s:slot=%d:hash=%x:context=%s:local=ack:peers=%s", t.localID, diagnosticKind, phase, decision.Slot, decision.Proposal.Hash, contextState, strings.Join(peerStates, ",")))
		}
		return firstErr
	}
	return quepaxa.ErrQuorumUnavailable
}

func foregroundLearnDiagnosticKind(value []byte) string {
	if _, checkpoint, err := quepaxa.DecodeCheckpointSeal(value); checkpoint && err == nil {
		return "checkpoint-seal"
	}
	if barrier, err := quepaxa.DecodeReadBarrier(value); barrier && err == nil {
		return "read-barrier"
	}
	return ""
}

func foregroundLearnDiagnosticErrorCategory(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	default:
		return "other"
	}
}

// sendFreezeDecision requires a durable old-configuration quorum. Terminal
// certification no longer depends on every individual freeze recorder surviving.
func (t *Transport) sendFreezeDecision(ctx context.Context, decision quepaxa.Decision) error {
	for _, summary := range decision.Summaries {
		if _, ok := t.members[summary.RecorderID]; !ok {
			return fmt.Errorf("freeze recorder %q is not a voter", summary.RecorderID)
		}
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		id  quepaxa.NodeID
		err error
	}
	results := make(chan result, len(t.members)-1)
	pending := 0
	for _, member := range t.members {
		if member.ID == t.localID {
			continue
		}
		pending++
		go func(member quepaxa.Member) {
			req := t.request(peerfb.OperationLearned)
			req.Decision = decisionToWire(decision)
			_, err := t.callQuorum(callCtx, member.ID, req)
			results <- result{id: member.ID, err: err}
		}(member)
	}
	successes := 1
	var firstErr error
	quorum := len(t.members)/2 + 1
	for range pending {
		result := <-results
		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
			}
			continue
		}
		successes++
		if successes >= quorum {
			return nil
		}
	}
	if firstErr != nil {
		return firstErr
	}
	return quepaxa.ErrQuorumUnavailable
}

// sendTerminalDecision retains the old decision envelope while requiring its
// durable installation by the next configuration. A newly added voter must
// acknowledge explicitly; an AB quorum cannot activate a replacement D that
// has not durably learned the terminal record.
func (t *Transport) sendTerminalDecision(ctx context.Context, decision quepaxa.Decision, old, next quepaxa.Cluster) error {
	oldMembers, nextMembers := old.MemberSet(), next.MemberSet()
	if decision.ConfigID != 0 && decision.ConfigID != old.ConfigID {
		return fmt.Errorf("terminal decision configuration mismatch")
	}
	union := old
	union.Members = append([]quepaxa.Member(nil), old.Members...)
	for _, member := range next.Members {
		if _, ok := oldMembers[member.ID]; !ok {
			union.Members = append(union.Members, member)
		}
	}
	peer, err := newTransportWithProfile(t.clusterID, t.localID, &union, t.peerToken, nil, t.expectedPolicy, t.managedProfile)
	if err != nil {
		return err
	}
	peer.quic = t.quic
	defer func() {
		if localtesthooks.Enabled {
			localtesthooks.Hit(fmt.Sprintf("network:terminal-decision:slot=%d:phase=close-start", decision.Slot))
		}
		_ = peer.Close()
	}()
	peer.adminToken = t.adminToken
	if peer.expectedPolicy == sqlpolicy.ManagedGraphRequestMaterializationPolicy {
		if err := peer.ValidateManagedGraphWireBudget(); err != nil {
			return err
		}
	}
	callCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		wg.Wait()
	}()
	type result struct {
		id  quepaxa.NodeID
		err error
	}
	results := make(chan result, len(union.Members)-1)
	pending := 0
	for _, member := range union.Members {
		if member.ID == t.localID {
			continue
		}
		pending++
		wg.Add(1)
		go func(member quepaxa.Member) {
			defer wg.Done()
			req := peer.request(peerfb.OperationLearned)
			req.Decision = decisionToWire(decision)
			trace := ""
			if localtesthooks.Enabled {
				trace = fmt.Sprintf("network:terminal-decision:slot=%d:peer=%s:old=%d:next=%d", decision.Slot, member.ID, old.ConfigID, next.ConfigID)
				localtesthooks.Hit(trace + ":phase=dispatch")
			}
			_, err := peer.callQuorum(callCtx, member.ID, req)
			if localtesthooks.Enabled {
				message := ""
				if err != nil {
					message = err.Error()
				}
				localtesthooks.Hit(trace + fmt.Sprintf(":phase=result:error=%q", message))
			}
			results <- result{id: member.ID, err: err}
		}(member)
	}
	oldAcks, nextAcks := 0, 0
	if _, ok := oldMembers[t.localID]; ok {
		oldAcks++
	}
	if _, ok := nextMembers[t.localID]; ok {
		nextAcks++
	}
	added := make(map[quepaxa.NodeID]bool)
	for id := range nextMembers {
		if _, ok := oldMembers[id]; !ok {
			added[id] = false
		}
	}
	var firstErr error
	for range pending {
		result := <-results
		_, required := added[result.id]
		if result.err != nil {
			if localtesthooks.Enabled {
				localtesthooks.Hit(fmt.Sprintf("network:terminal-decision:slot=%d:peer=%s:phase=consumed:error=%q:required-added=%t:old-acks=%d/%d:next-acks=%d/%d", decision.Slot, result.id, result.err, required, oldAcks, old.QuorumSize(), nextAcks, next.QuorumSize()))
			}
			if firstErr == nil {
				firstErr = result.err
			}
			if required {
				if localtesthooks.Enabled {
					localtesthooks.Hit(fmt.Sprintf("network:terminal-decision:slot=%d:phase=failed:reason=required-added-ack:peer=%s", decision.Slot, result.id))
				}
				return result.err
			}
			continue
		}
		if _, ok := oldMembers[result.id]; ok {
			oldAcks++
		}
		if _, ok := nextMembers[result.id]; ok {
			nextAcks++
		}
		if required {
			added[result.id] = true
		}
		if localtesthooks.Enabled {
			localtesthooks.Hit(fmt.Sprintf("network:terminal-decision:slot=%d:peer=%s:phase=consumed:required-added=%t:old-acks=%d/%d:next-acks=%d/%d", decision.Slot, result.id, required, oldAcks, old.QuorumSize(), nextAcks, next.QuorumSize()))
		}
		allAdded := true
		for _, acknowledged := range added {
			if !acknowledged {
				allAdded = false
				break
			}
		}
		if allAdded && oldAcks >= old.QuorumSize() && nextAcks >= next.QuorumSize() {
			if localtesthooks.Enabled {
				localtesthooks.Hit(fmt.Sprintf("network:terminal-decision:slot=%d:phase=complete:old-acks=%d/%d:next-acks=%d/%d:all-added=%t", decision.Slot, oldAcks, old.QuorumSize(), nextAcks, next.QuorumSize(), allAdded))
			}
			return nil
		}
	}
	if firstErr != nil {
		if localtesthooks.Enabled {
			localtesthooks.Hit(fmt.Sprintf("network:terminal-decision:slot=%d:phase=failed:reason=results-exhausted:old-acks=%d/%d:next-acks=%d/%d", decision.Slot, oldAcks, old.QuorumSize(), nextAcks, next.QuorumSize()))
		}
		return firstErr
	}
	if localtesthooks.Enabled {
		localtesthooks.Hit(fmt.Sprintf("network:terminal-decision:slot=%d:phase=failed:reason=quorum-unavailable:old-acks=%d/%d:next-acks=%d/%d", decision.Slot, oldAcks, old.QuorumSize(), nextAcks, next.QuorumSize()))
	}
	return quepaxa.ErrQuorumUnavailable
}

func decisionHasRecorder(decision quepaxa.Decision, id quepaxa.NodeID) bool {
	for _, summary := range decision.Summaries {
		if summary.RecorderID == id {
			return true
		}
	}
	return false
}
