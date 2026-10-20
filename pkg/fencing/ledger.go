package fencing

import "fmt"

// Ledger is one source generation's durable CAS value. All mutations must be
// persisted with the resourceVersion read alongside this value. Never replace a
// ledger after a conflict or discard an older incarnation when a Pod disappears.
type Ledger struct {
	SourceClusterID       string                 `json:"sourceClusterID"`
	ClosedBy              string                 `json:"closedBy,omitempty"`
	ClosedHash            string                 `json:"closedHash,omitempty"`
	ClosedNodes           map[string]StopRequest `json:"closedNodes,omitempty"`
	Runs                  map[string]Run         `json:"runs"`
	RequireRuntimeBinding bool                   `json:"requireRuntimeBinding,omitempty"`
}

type Run struct {
	Identity       Identity        `json:"identity"`
	Stop           *StopRequest    `json:"stop,omitempty"`
	Drain          *DrainReceipt   `json:"drain,omitempty"`
	Exited         bool            `json:"exited,omitempty"`
	NeverBoundExit *StopRequest    `json:"neverBoundExit,omitempty"`
	Runtime        *RuntimeBinding `json:"runtime,omitempty"`
}

func (l *Ledger) Register(identity Identity) error {
	if identity.SourceClusterID != l.SourceClusterID || identity.NodeID == "" || identity.PodUID == "" || identity.RunID == "" || identity.WALIdentity != "" {
		return fmt.Errorf("fencing: invalid registration")
	}
	if l.ClosedBy != "" {
		return ErrClosed
	}
	if _, closed := l.ClosedNodes[identity.NodeID]; closed {
		return ErrClosed
	}
	if l.Runs == nil {
		l.Runs = make(map[string]Run)
	}
	if old, exists := l.Runs[identity.RunID]; exists {
		// A caller may resolve an uncertain registration response, but cannot
		// reuse a bound run as authorization for another child spawn.
		if old.Identity == identity && old.Stop == nil {
			return nil
		}
		return fmt.Errorf("fencing: run ID already registered")
	}
	// Same-Pod container restart can continue its intact WAL, not erase history.
	for _, old := range l.Runs {
		if old.Identity.NodeID == identity.NodeID && old.Identity.PodUID != identity.PodUID && (!old.Exited || old.Drain == nil || !old.Drain.Clean()) {
			return fmt.Errorf("fencing: earlier incarnation has no clean termination proof")
		}
	}
	l.Runs[identity.RunID] = Run{Identity: identity}
	return nil
}

func (l *Ledger) Bind(identity Identity) error {
	run, exists := l.Runs[identity.RunID]
	if !exists || identity.WALIdentity == "" {
		return fmt.Errorf("fencing: unknown WAL registration")
	}
	old := identity
	old.WALIdentity = run.Identity.WALIdentity
	if old != run.Identity || (run.Identity.WALIdentity != "" && run.Identity.WALIdentity != identity.WALIdentity) {
		return fmt.Errorf("fencing: binding changed")
	}
	if l.ClosedBy != "" || run.Stop != nil {
		return ErrClosed
	}
	if l.RequireRuntimeBinding {
		if run.Runtime == nil {
			return fmt.Errorf("fencing: runtime enrollment required before WAL binding")
		}
		if err := run.Runtime.Validate(identity); err != nil {
			return err
		}
	}
	for _, old := range l.Runs {
		if old.Identity.RunID != identity.RunID && old.Identity.NodeID == identity.NodeID && old.Identity.WALIdentity != "" && old.Identity.WALIdentity != identity.WALIdentity {
			return fmt.Errorf("fencing: restarted voter must retain its historical WAL")
		}
	}
	run.Identity = identity
	l.Runs[identity.RunID] = run
	return nil
}

// CloseNodes fences stable voter slots, including subsequent Pod recreation,
// without stopping surviving voters. Empty nodeIDs is generation-wide.
func (l *Ledger) CloseNodes(operationID, requestHash string, nodeIDs []string) error {
	if len(nodeIDs) == 0 {
		return l.Close(operationID, requestHash)
	}
	if operationID == "" || requestHash == "" {
		return ErrClosed
	}
	if l.ClosedBy != "" {
		// An earlier pending voter operation may finish after escalation has
		// closed the generation. It cannot reopen or alter its original scope.
		for _, nodeID := range nodeIDs {
			old, ok := l.ClosedNodes[nodeID]
			if !ok || old.OperationID != operationID || old.RequestHash != requestHash {
				return ErrClosed
			}
		}
		return nil
	}
	for _, nodeID := range nodeIDs {
		if nodeID == "" {
			return fmt.Errorf("fencing: empty voter slot")
		}
		if old, ok := l.ClosedNodes[nodeID]; ok && old.OperationID == operationID && old.RequestHash != requestHash {
			return fmt.Errorf("fencing: immutable voter request hash changed")
		}
	}
	if l.ClosedNodes == nil {
		l.ClosedNodes = make(map[string]StopRequest)
	}
	for _, nodeID := range nodeIDs {
		l.ClosedNodes[nodeID] = StopRequest{OperationID: operationID, RequestHash: requestHash}
		for id, run := range l.Runs {
			if run.Identity.NodeID != nodeID {
				continue
			}
			if run.Stop != nil {
				continue
			}
			request := StopRequest{OperationID: operationID, RequestHash: requestHash, Identity: run.Identity}
			run.Stop = &request
			l.Runs[id] = run
		}
	}
	return nil
}

// Close permanently closes generation-wide startup admission before stop
// requests can be delivered. A different operation cannot reopen this authority.
func (l *Ledger) Close(operationID, requestHash string) error {
	if operationID == "" || requestHash == "" {
		return fmt.Errorf("fencing: empty operation")
	}
	if l.ClosedBy != "" && l.ClosedBy != operationID {
		return ErrClosed
	}
	if l.ClosedBy == operationID && l.ClosedHash != requestHash {
		return fmt.Errorf("fencing: immutable request hash changed")
	}
	l.ClosedBy = operationID
	l.ClosedHash = requestHash
	for id, run := range l.Runs {
		// Prior stop requests and their drain/exit evidence remain immutable.
		// Generation-wide closure can include a slot already terminally stopped
		// by an earlier voter fence without issuing a second process command.
		if run.Stop != nil {
			continue
		}
		request := StopRequest{OperationID: operationID, RequestHash: requestHash, Identity: run.Identity}
		run.Stop = &request
		l.Runs[id] = run
	}
	return nil
}

func (l *Ledger) RecordDrain(receipt DrainReceipt) error {
	run, exists := l.Runs[receipt.Request.Identity.RunID]
	if !exists || run.Stop == nil || *run.Stop != receipt.Request {
		return fmt.Errorf("fencing: unmatched drain")
	}
	if run.NeverBoundExit != nil {
		return fmt.Errorf("fencing: never-bound exit cannot become a drain")
	}
	if run.Drain != nil && *run.Drain != receipt {
		return fmt.Errorf("fencing: drain receipt cannot change")
	}
	run.Drain = &receipt
	l.Runs[run.Identity.RunID] = run
	return nil
}

// RecordExit is called only by a supervisor after the child has exited and the
// private process namespace contains no other process. Engine receipts alone
// must never call this method.
func (l *Ledger) RecordExit(request StopRequest) error {
	run, exists := l.Runs[request.Identity.RunID]
	if !exists || run.Identity.WALIdentity == "" || run.Stop == nil || *run.Stop != request || run.Drain == nil || !run.Drain.Clean() {
		return ErrPending
	}
	run.Exited = true
	l.Runs[request.Identity.RunID] = run
	return nil
}

// RecordNeverBoundExit requires a durable closed, never-authorized run and
// separately verified process termination. A lost successful WAL bind response
// cannot use this exception because the WAL identity is already persisted.
func (l *Ledger) RecordNeverBoundExit(request StopRequest) error {
	run, exists := l.Runs[request.Identity.RunID]
	if !exists || run.Identity.WALIdentity != "" || run.Drain != nil || request.Identity != run.Identity || run.Stop == nil || *run.Stop != request {
		return fmt.Errorf("fencing: never-bound exit is not proven")
	}
	if l.ClosedBy == "" {
		if _, closed := l.ClosedNodes[run.Identity.NodeID]; !closed {
			return ErrPending
		}
	}
	run.Exited = true
	copy := request
	run.NeverBoundExit = &copy
	l.Runs[request.Identity.RunID] = run
	return nil
}

// Complete requires exact current Pod inventory in addition to all historic
// runs. A missing registration cannot be inferred safe from Pod health.
func (l *Ledger) Complete(operationID, requestHash string, currentPodUIDs []string) bool {
	return l.CompleteNodes(operationID, requestHash, nil, currentPodUIDs)
}

func (l *Ledger) CompleteNodes(operationID, requestHash string, nodeIDs, currentPodUIDs []string) bool {
	if operationID == "" || requestHash == "" || len(l.Runs) == 0 {
		return false
	}
	wanted := make(map[string]bool)
	for _, id := range nodeIDs {
		closure, ok := l.ClosedNodes[id]
		if !ok || closure.OperationID != operationID || closure.RequestHash != requestHash {
			return false
		}
		wanted[id] = true
	}
	if len(wanted) == 0 && (l.ClosedBy != operationID || l.ClosedHash != requestHash) {
		return false
	}
	registered := make(map[string]bool)
	observed := make(map[string]bool)
	for _, run := range l.Runs {
		if len(wanted) > 0 && !wanted[run.Identity.NodeID] {
			continue
		}
		observed[run.Identity.NodeID] = true
		registered[run.Identity.PodUID] = true
		if run.Stop == nil || run.Stop.Identity != run.Identity || run.Stop.OperationID == "" || run.Stop.RequestHash == "" || !run.Exited {
			return false
		}
		if run.NeverBoundExit != nil {
			if run.Identity.WALIdentity != "" || run.Drain != nil || *run.NeverBoundExit != *run.Stop {
				return false
			}
		} else if run.Identity.WALIdentity == "" || run.Drain == nil || run.Drain.Request != *run.Stop || !run.Drain.Clean() {
			return false
		}
	}
	for id := range wanted {
		if !observed[id] {
			return false
		}
	}
	for _, uid := range currentPodUIDs {
		if !registered[uid] {
			return false
		}
	}
	return true
}
