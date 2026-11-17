package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/internal/sqlpolicy"
	"github.com/mrchypark/rhiza/pkg/materializer"
	"github.com/mrchypark/rhiza/pkg/network"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	sqlite3 "github.com/ncruces/go-sqlite3"
)

func (n *Node) catchUpArchive(ctx context.Context) error {
	if n.archive == nil || n.core == nil {
		return nil
	}
	if err := n.archive.Load(ctx); err != nil {
		return err
	}
	if seal, _, ok := n.archive.RecoveryBase(); ok && archiveRecoveryRequired(n.core.Tip(), seal.Index) {
		// The archive has trimmed the local next slot. Waiting for a peer to
		// report ErrCompacted only turns this into a periodic retry loop.
		return n.restoreArchiveCatchUp(ctx)
	}
	for n.core.Tip() < n.archive.Tip() {
		values, _, err := n.archive.DecisionsFrom(ctx, n.core.Tip()+1, 256)
		if err != nil || len(values) == 0 {
			if err == nil {
				err = fmt.Errorf("shared archive omitted slot %d", n.core.Tip()+1)
			}
			return err
		}
		for _, decision := range values {
			if err := n.validateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if err := n.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
	}
	return n.replayLocalDecisions(ctx)
}

func archiveRecoveryRequired(localTip, archiveBase quepaxa.Slot) bool {
	return archiveBase != 0 && localTip < archiveBase
}

func (n *Node) restoreArchiveCatchUp(ctx context.Context) (resultErr error) {
	ctx = localtesthooks.WithRecoveryPinCategory(ctx, localtesthooks.RecoveryPinOwnerNodeCatchup)
	n.recoveryMu.Lock()
	defer n.recoveryMu.Unlock()
	// A compacted peer means local history may no longer be sufficient for a
	// linearizable operation. Reject new client traffic before any remote I/O.
	n.ready.Store(false)
	if n.managedStartup != nil {
		return fmt.Errorf("managed checkpoint catch-up is unavailable until authorized restore is implemented")
	}
	if n.archive == nil || n.checkpoints == nil || n.core == nil || n.material == nil || n.server == nil {
		return fmt.Errorf("live checkpoint recovery is unavailable")
	}
	var pinNonce [8]byte
	if _, err := rand.Read(pinNonce[:]); err != nil {
		return err
	}
	owner := fmt.Sprintf("%s-%x", n.config.NodeID, pinNonce)
	snapshot, err := n.archive.BeginRecoverySnapshot(ctx, owner, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Carry only the recovery pin observer onto the fresh close context. The
		// background semantics, deadline, cancellation, and discarded error are
		// unchanged.
		closeCtx = localtesthooks.CarryRecoveryPinObserver(closeCtx, ctx)
		_ = snapshot.Close(closeCtx)
	}()
	seal, baseDecision, ok := snapshot.RecoveryBase()
	if !ok {
		return fmt.Errorf("shared archive has no checkpoint recovery base")
	}
	root, err := n.checkpoints.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
	if err != nil {
		return err
	}
	if root.Hash != seal.StateHash {
		return fmt.Errorf("certified checkpoint state hash mismatch")
	}
	checkpointPin, err := n.checkpoints.PinRecoveryRoot(ctx, root, owner, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = checkpointPin.Close(closeCtx)
	}()
	root, err = checkpointPin.Root()
	if err != nil {
		return err
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var renewMu sync.Mutex
	var renewFailure error
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(40 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if err := checkpointPin.Renew(workCtx, 2*time.Minute); err != nil {
					renewMu.Lock()
					renewFailure = err
					renewMu.Unlock()
					cancel()
					return
				}
				if err := snapshot.Renew(workCtx, 2*time.Minute); err != nil {
					renewMu.Lock()
					renewFailure = err
					renewMu.Unlock()
					cancel()
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		<-renewDone
	}()
	checkRenewFailure := func(err error) error {
		renewMu.Lock()
		defer renewMu.Unlock()
		if renewFailure != nil {
			return renewFailure
		}
		return err
	}
	defer func() { resultErr = checkRenewFailure(resultErr) }()
	dir, err := os.MkdirTemp(n.config.DataDir, ".rhiza-live-restore-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	files, err := n.checkpoints.DownloadAndVerifyRootFiles(workCtx, root, dir)
	if err != nil {
		return checkRenewFailure(err)
	}
	if err := n.validateCheckpointFiles(workCtx, files, uint64(seal.Index)); err != nil {
		return err
	}
	materialFiles := make([]materializer.CheckpointFile, 0, len(files))
	for _, file := range files {
		materialFiles = append(materialFiles, materializer.CheckpointFile{Role: materializer.CheckpointRole(file.Role), Path: file.Path})
	}
	resume, err := n.server.Quiesce(workCtx)
	if err != nil {
		return checkRenewFailure(err)
	}
	defer resume()
	if n.core.Tip() < seal.Index {
		if err := n.core.RestoreCheckpointBase(workCtx, seal, baseDecision); err != nil {
			return err
		}
	} else if prefix, ok := n.core.PrefixHash(seal.Index); !ok || prefix != seal.PrefixHash {
		return fmt.Errorf("local consensus prefix does not match checkpoint recovery base")
	}
	if n.material.Tip() < uint64(seal.Index) {
		if err := n.material.RestoreCheckpoint(workCtx, materialFiles); err != nil {
			return err
		}
	}
	for n.core.Tip() < snapshot.Tip() {
		values, _, err := snapshot.DecisionsFrom(workCtx, n.core.Tip()+1, 256)
		if err != nil || len(values) == 0 {
			if err == nil {
				err = fmt.Errorf("shared archive omitted slot %d", n.core.Tip()+1)
			}
			return err
		}
		for _, decision := range values {
			if err := n.validateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if err := n.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
	}
	if err := n.archive.Load(workCtx); err != nil {
		return err
	}
	for n.core.Tip() < n.archive.Tip() {
		values, _, err := n.archive.DecisionsFrom(workCtx, n.core.Tip()+1, 256)
		if err != nil || len(values) == 0 {
			if err == nil {
				err = fmt.Errorf("shared archive omitted slot %d", n.core.Tip()+1)
			}
			return err
		}
		for _, decision := range values {
			if err := n.validateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if err := n.core.AcceptCertifiedValues(values); err != nil {
			return err
		}
	}
	if quepaxa.Slot(n.material.Tip()) > n.core.Tip() {
		return fmt.Errorf("materialized slot %d is ahead of certified log tip %d", n.material.Tip(), n.core.Tip())
	}
	if err := n.replayLocalDecisions(workCtx); err != nil {
		return err
	}
	if n.material.Tip() > uint64(n.core.CompactionFloor()) {
		decision, ok := n.core.CertifiedValue(quepaxa.Slot(n.material.Tip()))
		if !ok {
			return fmt.Errorf("materialized slot %d has no recovered decision", n.material.Tip())
		}
		if err := n.material.ValidateTip(n.material.Tip(), decision.Value); err != nil {
			return err
		}
	}
	return nil
}

// Start opens the embedded engine and serves the optional public HTTP adapter.

func quarantineSQLite(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	suffix := fmt.Sprintf(".corrupt-%d", time.Now().UnixNano())
	if err := os.Rename(path, path+suffix); err != nil {
		return err
	}
	for _, extra := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + extra); err == nil {
			_ = os.Rename(path+extra, filepath.Clean(path+extra+suffix))
		}
	}
	return nil
}

func (n *Node) replayLocalDecisions(ctx context.Context) error {
	_, err := n.replayLocalDecisionsFrom(ctx)
	return err
}

// replayLocalDecisionsThrough applies one fixed continuous certified prefix.
// It is used by admission recovery after observing a stable Core tip; callers
// must not extend the bound if consensus advances while replay is in progress.
func (n *Node) replayLocalDecisionsThrough(ctx context.Context, through quepaxa.Slot) error {
	if through == 0 {
		return fmt.Errorf("bounded local replay requires a positive certified prefix")
	}
	_, err := n.replayLocalDecisionsTo(ctx, through)
	return err
}

// The diagnostic variant retains the exact failed local replay page under the
// same replay owner; callers must not reconstruct it from a later live tip.
func (n *Node) replayLocalDecisionsFrom(ctx context.Context) (quepaxa.Slot, error) {
	return n.replayLocalDecisionsTo(ctx, 0)
}

func (n *Node) replayLocalDecisionsTo(ctx context.Context, through quepaxa.Slot) (quepaxa.Slot, error) {
	// Archive catch-up and checkpoint verification can both request local
	// replay. Keep the tip read, decision selection, apply, and restore-suffix
	// state transition under one owner so neither path races or replays stale
	// state concurrently.
	n.replayMu.Lock()
	defer n.replayMu.Unlock()
	for {
		from := quepaxa.Slot(n.material.Tip() + 1)
		if through != 0 && from > through {
			localtesthooks.Hit("node:replay-local-decisions-through:applied")
			return 0, nil
		}
		limit := 256
		if through != 0 {
			remaining := through - from + 1
			if remaining < quepaxa.Slot(limit) {
				limit = int(remaining)
			}
		}
		decisions, tip, err := n.core.DecisionsFrom(from, limit)
		if err != nil {
			return from, err
		}
		if len(decisions) == 0 && through != 0 && from <= through {
			return from, fmt.Errorf("certified local replay made no progress through slot %d", through)
		}
		// HA certified hints may be memory-only. Local cannot accept hints and
		// its reclaim callback already owns the non-reentrant execution gate.
		if !n.core.LocalMode() && len(decisions) != 0 {
			if err := n.core.EnsureDurableThrough(ctx, decisions[len(decisions)-1].Slot); err != nil {
				return from, err
			}
		}
		if n.localRestoreSuffixPending && localRestoreSuffixCrashBoundary != nil {
			for _, decision := range decisions {
				if err := n.material.ApplyBatch(ctx, []quepaxa.DecidedValue{decision}); err != nil {
					return decision.Slot, err
				}
				localRestoreSuffixCrashBoundary(uint64(decision.Slot))
			}
		} else if err := n.material.ApplyBatch(ctx, decisions); err != nil {
			return from, err
		}
		applied := quepaxa.Slot(n.material.Tip())
		if through != 0 && applied >= through {
			if applied >= tip {
				n.localRestoreSuffixPending = false
			}
			localtesthooks.Hit("node:replay-local-decisions-through:applied")
			return 0, nil
		}
		if applied >= tip {
			n.localRestoreSuffixPending = false
			if through == 0 {
				return 0, nil
			}
			if applied < through {
				return from, fmt.Errorf("certified local history ended before fixed replay prefix %d", through)
			}
		}
	}
}

// A learner pulls certified history without running voter recovery. Its WAL
// identity remains distinct, and only a terminal decision grants voting rights.
func (n *Node) catchUpMembership(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) error {
	if n.config.EnableReconfiguration {
		current := n.core.CurrentCluster()
		cluster = &current
	}
	if n.config.Learner == nil || n.core.IsVoter() {
		return n.catchUpQuorum(ctx, transport, cluster)
	}
	var last error = quepaxa.ErrQuorumUnavailable
	for _, member := range cluster.Members {
		if member.ID == n.core.NodeID() {
			continue
		}
		if last = n.catchUpPeer(ctx, transport, member.ID); last == nil {
			if err := n.core.EnsureDurableThrough(ctx, n.core.Tip()); err != nil {
				return err
			}
			return n.replayLocalDecisions(ctx)
		}
	}
	return last
}

// catchUpPreparedArchiveSuffix applies only the already-certified archive
// suffix after the exact prepared view has been verified and persisted as the
// adoption boundary. The prepared view remains the recovery authority; archive
// state is only a source of independently validated DecidedValues.
func (n *Node) catchUpPreparedArchiveSuffix(ctx context.Context, adoptedThrough quepaxa.Slot) error {
	if n == nil || n.config == nil || n.config.Learner == nil || n.managedStartup == nil || n.managedStartup.Prepared == nil || n.core == nil || n.material == nil || n.archive == nil {
		return fmt.Errorf("prepared learner archive catch-up is unavailable")
	}
	if n.core.Tip() != adoptedThrough || quepaxa.Slot(n.material.Tip()) != adoptedThrough {
		return fmt.Errorf("prepared learner did not remain at its adopted cut")
	}
	target := n.archive.Tip()
	if target < adoptedThrough {
		target = adoptedThrough
	}
	for n.core.Tip() < target {
		from := n.core.Tip() + 1
		values, _, err := n.archive.DecisionsFrom(ctx, from, 256)
		if err != nil {
			return fmt.Errorf("read prepared learner certified suffix from %d: %w", from, err)
		}
		if len(values) == 0 || values[0].Slot != from {
			return fmt.Errorf("prepared learner archive omitted certified slot %d", from)
		}
		limit := len(values)
		for i, decision := range values {
			if decision.Slot > target {
				limit = i
				break
			}
			if err := n.validateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if limit == 0 {
			return fmt.Errorf("prepared learner archive omitted certified slot %d", from)
		}
		if err := n.core.AcceptCertifiedValues(values[:limit]); err != nil {
			return fmt.Errorf("accept prepared learner certified suffix: %w", err)
		}
		if localtesthooks.Enabled {
			localtesthooks.Hit("node:managed-prepared:suffix-page-accepted")
		}
	}
	if n.core.Tip() > adoptedThrough {
		if err := n.core.EnsureDurableThrough(ctx, n.core.Tip()); err != nil {
			return fmt.Errorf("sync prepared learner certified suffix: %w", err)
		}
		if err := n.replayLocalDecisions(ctx); err != nil {
			return fmt.Errorf("replay prepared learner certified suffix: %w", err)
		}
	}
	if n.core.Tip() != target || quepaxa.Slot(n.material.Tip()) != target {
		return fmt.Errorf("prepared learner did not finish the fixed archived suffix")
	}
	return nil
}

func (n *Node) startCatchUp(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) {
	if n.managedStartup != nil && (n.managedStartup.Prepared != nil || n.managedStartup.verifiedAdoption != nil) && n.config.Learner != nil && isManagedAppendOnlyProtocol(n.managedStartup.Identity.ApplicationProtocol) {
		if n.managedStartup.promotionReady == nil {
			return
		}
		select {
		case <-n.managedStartup.promotionReady:
		case <-ctx.Done():
			return
		}
	}
	n.observeCatchUp(n.catchUpMembership(ctx, transport, cluster))
	var round uint64
	for {
		delay := syncInterval(n.config.NodeID, round)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		case <-n.catchUpWake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		if n.config.EnableReconfiguration {
			current := n.core.CurrentCluster()
			cluster = &current
		}
		if !n.ready.Load() {
			n.observeCatchUp(n.catchUpMembership(ctx, transport, cluster))
		} else if sources := syncSources(n.config.NodeID, cluster.Members, round); len(sources) > 0 {
			attempts, syncErr := runOperationSyncRound(sources, func(source quepaxa.NodeID) error {
				return n.catchUpPeer(ctx, transport, source)
			}, n.material.Tip)
			if syncErr != nil {
				logOperationSyncFailure(n.config.NodeID, round, attempts, log.Print)
			}
			// A compacted peer can force checkpoint recovery from catchUpPeer.
			// Only a fresh quorum round may make that node ready again.
			if !n.ready.Load() {
				n.observeCatchUp(n.catchUpMembership(ctx, transport, cluster))
			}
		}
		round++
	}
}

type catchUpDiagnosticError struct {
	stage           string
	from, remoteTip quepaxa.Slot
	remoteTipKnown  bool
	cause           error
}

func (e *catchUpDiagnosticError) Error() string {
	return fmt.Sprintf("catch-up stage=%s from=%d remote_tip_known=%t remote_tip=%d: %v", e.stage, e.from, e.remoteTipKnown, e.remoteTip, e.cause)
}
func (e *catchUpDiagnosticError) Unwrap() error { return e.cause }

type catchUpAttempt struct {
	source           quepaxa.NodeID
	startTip, endTip uint64
	elapsed          time.Duration
	err              error
}

// This is the existing sequential loop: no retry, shared cursor or new policy.
func runOperationSyncRound(sources []quepaxa.NodeID, attempt func(quepaxa.NodeID) error, tip func() uint64) ([]catchUpAttempt, error) {
	var attempts []catchUpAttempt
	var last error
	for _, source := range sources {
		startTip, started := tip(), time.Now()
		last = attempt(source)
		if last == nil {
			return nil, nil
		}
		attempts = append(attempts, catchUpAttempt{source: source, startTip: startTip, endTip: tip(), elapsed: time.Since(started), err: last})
	}
	return attempts, last
}

func catchUpErrorClass(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, quepaxa.ErrCompacted):
		return "compacted"
	case errors.Is(err, quepaxa.ErrQuorumUnavailable):
		return "quorum"
	case errors.Is(err, sqlpolicy.ErrIncompatible):
		return "policy"
	default:
		var sqlite sqlite3.ExtendedErrorCode
		var path *os.PathError
		switch {
		case errors.As(err, &sqlite):
			return "sqlite"
		case errors.As(err, &path):
			return "filesystem"
		case errors.Is(err, network.ErrOverloaded):
			return "overloaded"
		case errors.Is(err, network.ErrInvalidRequest):
			return "invalid-request"
		default:
			return "unknown"
		}
	}
}

func catchUpCauseCode(err error) (code, extended int, known bool, fileOp string) {
	var sqlite sqlite3.ExtendedErrorCode
	if errors.As(err, &sqlite) {
		return int(sqlite.Code()), int(sqlite), true, "none"
	}
	var path *os.PathError
	if errors.As(err, &path) {
		fileOp = "unknown"
		switch path.Op {
		case "open", "read", "write", "close", "rename", "remove", "stat", "lstat", "mkdir", "sync":
			fileOp = path.Op
		}
		var errno syscall.Errno
		if errors.As(path.Err, &errno) {
			return int(errno), 0, true, fileOp
		}
		return 0, 0, false, fileOp
	}
	return 0, 0, false, "none"
}

func logOperationSyncFailure(local quepaxa.NodeID, round uint64, attempts []catchUpAttempt, emit func(...any)) {
	for i, attempt := range attempts {
		stage, from, remoteTip, known := "unknown", quepaxa.Slot(0), quepaxa.Slot(0), false
		var failure *catchUpDiagnosticError
		if errors.As(attempt.err, &failure) {
			switch failure.stage {
			case "local-replay", "fetch", "restore-archive", "validate", "accept", "apply":
				stage = failure.stage
			}
			from, remoteTip, known = failure.from, failure.remoteTip, failure.remoteTipKnown
		}
		rpc := network.PeerRPCDiagnostic{Phase: "unknown"}
		rpcKnown := false
		var transport interface {
			RPCDiagnostic() network.PeerRPCDiagnostic
		}
		if errors.As(attempt.err, &transport) {
			rpcKnown = true
			rpc = transport.RPCDiagnostic()
			switch rpc.Phase {
			case "connection-gate", "peer-identity", "dial", "handshake", "stream-open", "request-write", "request-close", "response-read", "response-decode", "response-code":
			default:
				rpc.Phase = "unknown"
			}
		}
		// One bounded record per source, emitted ONLY for the existing all-failed
		// event. Ordinary clusters have no 64-member bound; never truncate peers.
		class := catchUpErrorClass(attempt.err)
		code, extended, codeKnown, fileOp := 0, 0, false, "none"
		if class == "sqlite" || class == "filesystem" {
			code, extended, codeKnown, fileOp = catchUpCauseCode(attempt.err)
		}
		emit(fmt.Sprintf("operation sync deferred: error_class=%s error_code_known=%t error_code=%d error_extended=%d file_op=%s node_hash=%x round=%d attempt=%d count=%d source_hash=%x start_tip=%d end_tip=%d elapsed_us=%d stage=%s from=%d remote_tip_known=%t remote_tip=%d rpc_known=%t rpc_phase=%s rpc_elapsed_us=%d connection_known=%t connection_us=%d request_known=%t request_us=%d rpc_total_known=%t rpc_total_us=%d gate_wait_known=%t gate_wait_us=%d mutex_wait_known=%t mutex_wait_us=%d dial_measured=%t dial_us=%d dial_deadline_known=%t dial_remaining_us=%d", class, codeKnown, code, extended, fileOp, sha256.Sum256([]byte(local)), round, i+1, len(attempts), sha256.Sum256([]byte(attempt.source)), attempt.startTip, attempt.endTip, attempt.elapsed.Microseconds(), stage, from, known, remoteTip, rpcKnown, rpc.Phase, rpc.Elapsed.Microseconds(), rpc.ConnectionKnown, rpc.ConnectionElapsed.Microseconds(), rpc.RequestKnown, rpc.RequestElapsed.Microseconds(), rpc.TotalKnown, rpc.TotalElapsed.Microseconds(), rpc.GateWaitKnown, rpc.GateWait.Microseconds(), rpc.MutexWaitKnown, rpc.MutexWait.Microseconds(), rpc.DialMeasured, rpc.DialElapsed.Microseconds(), rpc.DialDeadlineKnown, rpc.DialRemaining.Microseconds()))
	}
}

// wakeCatchUp is called by the foreground server when a peer has compacted
// history needed by this node. The one-slot channel coalesces concurrent
// callers; startCatchUp is the only recovery worker.
func (n *Node) wakeCatchUp() {
	n.ready.Store(false)
	if n.catchUpWake == nil {
		return
	}
	select {
	case n.catchUpWake <- struct{}{}:
	default:
	}
}

func syncInterval(nodeID quepaxa.NodeID, round uint64) time.Duration {
	hash := sha256.Sum256([]byte(fmt.Sprintf("rhiza-sync-jitter:%s:%d", nodeID, round)))
	return time.Duration(900+binary.BigEndian.Uint16(hash[:2])%201) * time.Millisecond
}

func syncSources(nodeID quepaxa.NodeID, members []quepaxa.Member, round uint64) []quepaxa.NodeID {
	peers := make([]quepaxa.NodeID, 0, len(members)-1)
	for _, member := range members {
		if member.ID != nodeID {
			peers = append(peers, member.ID)
		}
	}
	if len(peers) == 0 {
		return nil
	}
	seed := sha256.Sum256([]byte(fmt.Sprintf("rhiza-sync-peer:%s:%d", nodeID, round/uint64(len(peers)))))
	for i := len(peers) - 1; i > 0; i-- {
		j := int(binary.BigEndian.Uint64(seed[(i*8)%24:]) % uint64(i+1))
		peers[i], peers[j] = peers[j], peers[i]
	}
	first := int(round % uint64(len(peers)))
	return append(peers[first:], peers[:first]...)
}

func (n *Node) observeCatchUp(err error) {
	if err != nil {
		// A transient peer timeout must not make an already caught-up node
		// reject traffic; quorum operations enforce their own availability.
		log.Printf("quorum catch-up failed: %v", err)
		return
	}
	if localtesthooks.Enabled {
		localtesthooks.Hit("node:catchup:before-ready:node=" + string(n.config.NodeID))
	}
	n.ready.Store(true)
	localtesthooks.Hit("node:catchup:after-ready:node=" + string(n.config.NodeID))
}

func hasUsablePeerSuffix(applied quepaxa.Slot, successes, quorum int, best *network.DecisionsResponse) bool {
	return successes >= quorum && best != nil && best.Tip > applied
}

func (n *Node) catchUpQuorum(ctx context.Context, transport *network.Transport, cluster *quepaxa.Cluster) error {
	for {
		if err := n.replayLocalDecisions(ctx); err != nil {
			return err
		}
		applied := quepaxa.Slot(n.material.Tip())
		type result struct {
			response network.DecisionsResponse
			err      error
		}
		results := make(chan result, len(cluster.Members)-1)
		// Initial QUIC connection establishment can exceed the steady-state RPC
		// budget after all peers restart and DNS endpoints are republished.
		roundCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		pending := 0
		for _, member := range cluster.Members {
			if member.ID == n.config.NodeID {
				continue
			}
			pending++
			go func(source quepaxa.NodeID) {
				response, err := transport.FetchDecisions(roundCtx, source, applied+1, 128)
				results <- result{response: response, err: err}
			}(member.ID)
		}
		successes := 1 // local recorder
		var best *network.DecisionsResponse
		compacted := false
		var grace *time.Timer
		for pending > 0 {
			var graceC <-chan time.Time
			if successes >= cluster.QuorumSize() {
				if grace == nil {
					grace = time.NewTimer(10 * time.Millisecond)
				}
				graceC = grace.C
			}
			select {
			case <-roundCtx.Done():
				pending = 0
			case <-graceC:
				pending = 0
			case result := <-results:
				pending--
				if errors.Is(result.err, quepaxa.ErrCompacted) {
					compacted = true
				} else if result.err == nil {
					successes++
					if best == nil || result.response.Tip > best.Tip {
						copy := result.response
						best = &copy
					}
				}
			}
		}
		if grace != nil {
			grace.Stop()
		}
		cancel()
		peerSuffix := hasUsablePeerSuffix(applied, successes, cluster.QuorumSize(), best)
		if compacted && !peerSuffix {
			if err := n.restoreArchiveCatchUp(ctx); err != nil {
				return err
			}
			continue
		}
		if successes < cluster.QuorumSize() {
			return quepaxa.ErrQuorumUnavailable
		}
		if !peerSuffix {
			return nil
		}
		expected := applied + 1
		for _, decision := range best.Decisions {
			if decision.Slot != expected {
				return fmt.Errorf("catch-up gap: expected=%d got=%d", expected, decision.Slot)
			}
			expected++
		}
		for _, decision := range best.Decisions {
			if err := n.validateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if err := n.core.AcceptCertifiedValues(best.Decisions); err != nil {
			return err
		}
		if err := n.material.ApplyBatch(ctx, best.Decisions); err != nil {
			return err
		}
		if len(best.Decisions) == 0 {
			return fmt.Errorf("catch-up source reported tip %d without slot %d", best.Tip, expected)
		}
	}
}

func (n *Node) catchUpPeer(ctx context.Context, transport *network.Transport, source quepaxa.NodeID) (result error) {
	stage, from := "local-replay", quepaxa.Slot(0)
	var remoteTip quepaxa.Slot
	var remoteTipKnown bool
	defer func() {
		if result != nil {
			result = &catchUpDiagnosticError{stage: stage, from: from, remoteTip: remoteTip, remoteTipKnown: remoteTipKnown, cause: result}
		}
	}()
	for {
		stage, from, remoteTip, remoteTipKnown = "local-replay", quepaxa.Slot(n.material.Tip())+1, 0, false
		localtesthooks.Hit("node:catchup-peer:phase=before-local-replay:node=" + string(n.config.NodeID))
		if replayFrom, err := n.replayLocalDecisionsFrom(ctx); err != nil {
			from = replayFrom
			return err
		}
		stage, from = "fetch", quepaxa.Slot(n.material.Tip())+1
		pageCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
		response, err := transport.FetchDecisions(pageCtx, source, from, 128)
		cancel()
		if err != nil {
			if errors.Is(err, quepaxa.ErrCompacted) {
				stage = "restore-archive"
				if err := n.restoreArchiveCatchUp(ctx); err != nil {
					return err
				}
				continue
			}
			return err
		}
		remoteTip, remoteTipKnown = response.Tip, true
		if response.Tip < from {
			return nil
		}
		stage = "validate"
		if len(response.Decisions) == 0 || response.Decisions[0].Slot != from {
			return fmt.Errorf("operation sync source %s omitted slot %d", source, from)
		}
		for _, decision := range response.Decisions {
			if err := n.validateExecutionPolicy(decision.Value); err != nil {
				return err
			}
		}
		if localtesthooks.Enabled {
			localtesthooks.Hit(fmt.Sprintf("node:catchup-peer:phase=before-accept:node=%s:source=%s:from=%d:tip=%d", n.core.NodeID(), source, from, response.Tip))
		}
		stage = "accept"
		if err := n.core.AcceptCertifiedValues(response.Decisions); err != nil {
			return err
		}
		stage = "apply"
		if err := n.material.ApplyBatch(ctx, response.Decisions); err != nil {
			return err
		}
		if quepaxa.Slot(n.material.Tip()) >= response.Tip {
			return nil
		}
	}
}

// Shutdown gracefully shuts down the node.
