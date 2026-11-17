package rhiza

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"time"

	"github.com/mrchypark/rhiza/internal/localtesthooks"
	"github.com/mrchypark/rhiza/pkg/checkpoint"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func (r *ReadReplica) advanceLearnerCheckpoint(ctx context.Context) error {
	if failure := r.checkpointFailure.Load(); failure != nil {
		return failure.err
	}
	if r.checkpointJob == nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().Before(r.nextCheckpointProbe) {
			return nil
		}
		r.startLearnerCheckpointProbe()
		return nil
	}
	job := r.checkpointJob
	if job.candidate == nil {
		select {
		case result := <-job.result:
			if result.err != nil {
				r.checkpointProbeFailed(result.err)
				r.stopLearnerCheckpointJob()
				return nil
			}
			if result.candidate == nil {
				r.checkpointProbeFailures = 0
				r.checkpointError = nil
				r.nextCheckpointProbe = time.Now().Add(learnerCheckpointProbe)
				r.stopLearnerCheckpointJob()
				return nil
			}
			job.candidate = result.candidate
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	return r.finishLearnerCheckpoint(ctx, job)
}

func (r *ReadReplica) startLearnerCheckpointProbe() {
	jobCtx, cancel := context.WithCancel(r.ctx)
	job := &learnerCheckpointJob{
		ctx: jobCtx, cancel: cancel, done: make(chan struct{}), result: make(chan learnerCheckpointResult, 1), finish: make(chan struct{}),
	}
	r.checkpointJob = job
	r.nextCheckpointProbe = time.Now().Add(learnerCheckpointProbe)
	go r.verifyLearnerCheckpoint(jobCtx, job)
}

func (r *ReadReplica) verifyLearnerCheckpoint(ctx context.Context, job *learnerCheckpointJob) {
	defer close(job.done)
	owner, err := replicaOwner(r.config.ReplicaID)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		return
	}
	snapshot, err := r.archive.BeginRecoverySnapshot(ctx, owner, learnerCheckpointLease)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		return
	}
	seal, decision, ok := snapshot.RecoveryBase()
	if !ok {
		job.result <- learnerCheckpointResult{err: fmt.Errorf("published archive has no recovery checkpoint base")}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	if seal.Index == 0 || decision.Slot <= seal.Index {
		job.result <- learnerCheckpointResult{err: fmt.Errorf("published recovery base has invalid seal decision slot")}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	seal = cloneReplicaSeal(seal)
	decision = cloneReplicaDecision(decision)
	manager := checkpoint.NewManager(r.bucket, path.Join(r.config.ObjStorePrefix, r.config.ClusterID), "", seal.ConfigID)
	root, err := manager.OpenRoot(ctx, uint64(seal.Index), seal.RootHash)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	if root.Index != uint64(seal.Index) || root.ConfigID != seal.ConfigID || root.Hash != seal.StateHash {
		job.result <- learnerCheckpointResult{err: fmt.Errorf("published recovery root does not match its certified seal")}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	owner, err = replicaOwner(r.config.ReplicaID)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	leaseStart := time.Now()
	pin, err := manager.PinRecoveryRoot(ctx, root, owner, learnerCheckpointLease)
	if err != nil {
		job.result <- learnerCheckpointResult{err: err}
		_ = closeRecoverySnapshot(snapshot)
		return
	}
	job.mu.Lock()
	job.deadline = leaseStart.Add(learnerCheckpointLease)
	job.mu.Unlock()
	archiveCloseErr := closeRecoverySnapshot(snapshot)
	renewDone := make(chan struct{})
	job.stopRenew, job.renewDone = make(chan struct{}), renewDone
	go r.renewLearnerCheckpointPin(ctx, job, pin, renewDone)
	closePin := func() {
		job.cancel()
		r.haltLearnerCheckpointRenewal(job)
		job.cleanupErr = errors.Join(job.cleanupErr, closeCheckpointPin(pin), archiveCloseErr)
	}
	if archiveCloseErr != nil {
		closePin()
		job.result <- learnerCheckpointResult{err: archiveCloseErr}
		return
	}
	if err := manager.Verify(ctx, uint64(seal.Index), seal.RootHash, seal.StateHash); err != nil {
		closePin()
		job.result <- learnerCheckpointResult{err: err}
		return
	}
	candidate := &learnerCheckpointCandidate{seal: seal, decision: cloneReplicaDecision(decision), manager: manager}
	job.result <- learnerCheckpointResult{candidate: candidate}
	select {
	case <-job.finish:
	case <-ctx.Done():
		job.mu.Lock()
		compacting := job.phase == learnerCheckpointCompacting
		job.mu.Unlock()
		if compacting {
			<-job.finish
		}
	}
	closePin()
}

func (r *ReadReplica) renewLearnerCheckpointPin(ctx context.Context, job *learnerCheckpointJob, pin *checkpoint.RecoveryPin, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(learnerCheckpointRenew)
	defer ticker.Stop()
	ctxDone := ctx.Done()
	for {
		select {
		case <-ctxDone:
			job.mu.Lock()
			compacting := job.phase == learnerCheckpointCompacting
			job.mu.Unlock()
			if !compacting {
				return
			}
			ctxDone = nil
		case <-job.stopRenew:
			return
		case <-ticker.C:
			started := time.Now()
			localtesthooks.Hit("learner:checkpoint-pin-renewal-started")
			err := pin.Renew(ctx, learnerCheckpointLease)
			job.mu.Lock()
			if err == nil && started.Before(job.deadline) && time.Now().Before(job.deadline) {
				job.deadline = started.Add(learnerCheckpointLease)
				job.mu.Unlock()
				continue
			}
			if err == nil {
				err = fmt.Errorf("checkpoint recovery pin renewal completed after the confirmed lease deadline")
			}
			localtesthooks.Hit("learner:checkpoint-pin-renewal-failed")
			job.leaseLost = true
			job.leaseErr = err
			phase := job.phase
			job.mu.Unlock()
			if phase == learnerCheckpointCompacting {
				r.latchLearnerCheckpointFailure(fmt.Errorf("checkpoint pin lost during WAL compaction: %w", err))
			}
			if phase != learnerCheckpointCompacting {
				job.cancel()
			}
			return
		}
	}
}

func (r *ReadReplica) haltLearnerCheckpointRenewal(job *learnerCheckpointJob) {
	if job.renewDone == nil {
		return
	}
	job.stopRenewOnce.Do(func() { close(job.stopRenew) })
	<-job.renewDone
}

func cloneReplicaSeal(seal quepaxa.CheckpointSeal) quepaxa.CheckpointSeal {
	seal.NextLeaderOrder = slices.Clone(seal.NextLeaderOrder)
	seal.FollowingLeaderOrder = slices.Clone(seal.FollowingLeaderOrder)
	if seal.Membership != nil {
		membership := *seal.Membership
		membership.Genesis.Members = slices.Clone(membership.Genesis.Members)
		membership.Transitions = slices.Clone(membership.Transitions)
		for i := range membership.Transitions {
			membership.Transitions[i].Freeze = cloneReplicaDecision(membership.Transitions[i].Freeze)
			membership.Transitions[i].Terminal = cloneReplicaDecision(membership.Transitions[i].Terminal)
		}
		if membership.Abort != nil {
			abort := *membership.Abort
			abort.Freeze = cloneReplicaDecision(abort.Freeze)
			abort.Terminal = cloneReplicaDecision(abort.Terminal)
			membership.Abort = &abort
		}
		seal.Membership = &membership
	}
	return seal
}

func cloneReplicaDecision(value quepaxa.DecidedValue) quepaxa.DecidedValue {
	value.Value = append([]byte(nil), value.Value...)
	value.Certificate = append(value.Certificate[:0:0], value.Certificate...)
	return value
}

func (r *ReadReplica) finishLearnerCheckpoint(ctx context.Context, job *learnerCheckpointJob) error {
	candidate := job.candidate
	if candidate == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	job.mu.Lock()
	leaseErr := job.leaseErr
	leaseOK := !job.leaseLost && time.Now().Before(job.deadline)
	job.mu.Unlock()
	if !leaseOK {
		if leaseErr == nil {
			leaseErr = fmt.Errorf("checkpoint recovery pin lease expired before adoption")
		}
		r.checkpointProbeFailed(leaseErr)
		r.stopLearnerCheckpointJob()
		return nil
	}
	if r.core.Tip() < candidate.decision.Slot || quepaxa.Slot(r.material.Tip()) < candidate.decision.Slot {
		return nil // The seal is certified remotely but not yet applied locally.
	}
	if err := r.archive.Load(ctx); err != nil {
		r.checkpointError = err
		return nil
	}
	currentSeal, _, hasCurrent := r.archive.RecoveryBase()
	if !hasCurrent || currentSeal.ConfigID != candidate.seal.ConfigID || currentSeal.Index < candidate.seal.Index ||
		(currentSeal.Index == candidate.seal.Index && currentSeal.RootHash != candidate.seal.RootHash) {
		r.checkpointProbeFailed(fmt.Errorf("published archive recovery base no longer includes the pinned candidate"))
		r.stopLearnerCheckpointJob()
		return nil
	}
	if candidate.seal.Index <= r.core.CompactionFloor() {
		r.checkpointProbeFailures = 0
		r.checkpointError = nil
		r.nextCheckpointProbe = time.Now().Add(learnerCheckpointProbe)
		r.stopLearnerCheckpointJob()
		return nil
	}
	localDecision, exists := r.core.CertifiedValue(candidate.decision.Slot)
	if !exists || localDecision.Hash != candidate.decision.Hash || !slices.Equal(localDecision.Value, candidate.decision.Value) ||
		!slices.Equal(localDecision.Certificate, candidate.decision.Certificate) {
		r.checkpointProbeFailed(fmt.Errorf("checkpoint seal decision is not the locally applied certified value"))
		r.stopLearnerCheckpointJob()
		return nil
	}
	previousManager := r.checkpoints
	r.checkpoints = candidate.manager
	defer func() { r.checkpoints = previousManager }()
	if err := r.core.ValidateCheckpointBase(job.ctx, candidate.seal, candidate.decision); err != nil {
		r.checkpointProbeFailed(err)
		r.stopLearnerCheckpointJob()
		return nil
	}
	job.mu.Lock()
	if job.leaseLost || !time.Now().Before(job.deadline) {
		leaseErr := job.leaseErr
		job.mu.Unlock()
		if leaseErr == nil {
			leaseErr = fmt.Errorf("checkpoint recovery pin lease expired before prepare")
		}
		r.checkpointProbeFailed(leaseErr)
		r.stopLearnerCheckpointJob()
		return nil
	}
	job.phase = learnerCheckpointPreparing
	job.mu.Unlock()
	if err := r.core.PrepareCheckpoint(job.ctx, candidate.seal); err != nil {
		failure := fmt.Errorf("checkpoint prepare outcome requires reopen: %w", err)
		r.latchLearnerCheckpointFailure(failure)
		r.stopLearnerCheckpointJob()
		return failure
	}
	job.mu.Lock()
	if job.leaseLost || !time.Now().Before(job.deadline) {
		leaseErr := job.leaseErr
		job.mu.Unlock()
		if leaseErr == nil {
			leaseErr = fmt.Errorf("checkpoint recovery pin lease expired after prepare")
		}
		failure := fmt.Errorf("checkpoint prepared but pin was lost before compaction; reopen required: %w", leaseErr)
		r.latchLearnerCheckpointFailure(failure)
		r.stopLearnerCheckpointJob()
		return failure
	}
	job.phase = learnerCheckpointCompacting
	job.mu.Unlock()
	compactErr := r.core.CompactThrough(candidate.seal.Index, candidate.seal.RootHash)
	localtesthooks.Hit("learner:checkpoint-compaction-finished")
	r.haltLearnerCheckpointRenewal(job)
	job.mu.Lock()
	leaseLost, leaseErr := job.leaseLost, job.leaseErr
	job.phase = learnerCheckpointVerifying
	job.mu.Unlock()
	if compactErr != nil {
		failure := fmt.Errorf("checkpoint compaction outcome requires reopen: %w", compactErr)
		r.latchLearnerCheckpointFailure(failure)
		r.stopLearnerCheckpointJob()
		return failure
	}
	r.syncedHead = nil
	if leaseLost {
		failure := fmt.Errorf("checkpoint floor committed after pin lease loss; reopen required: %w", leaseErr)
		r.latchLearnerCheckpointFailure(failure)
		r.stopLearnerCheckpointJob()
		return failure
	}
	r.checkpointProbeFailures = 0
	r.checkpointError = nil
	r.nextCheckpointProbe = time.Now().Add(learnerCheckpointProbe)
	r.stopLearnerCheckpointJob()
	return nil
}

func (r *ReadReplica) checkpointProbeFailed(err error) {
	r.checkpointError = err
	r.checkpointProbeFailures++
	delays := [...]time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second, 300 * time.Second}
	index := r.checkpointProbeFailures - 1
	if index >= len(delays) {
		index = len(delays) - 1
	}
	r.nextCheckpointProbe = time.Now().Add(delays[index])
}

func (r *ReadReplica) stopLearnerCheckpointJob() error {
	job := r.checkpointJob
	if job == nil {
		return nil
	}
	r.checkpointJob = nil
	job.cancel()
	close(job.finish)
	<-job.done
	if job.cleanupErr != nil {
		r.checkpointError = job.cleanupErr
	}
	return job.cleanupErr
}

func (r *ReadReplica) latchLearnerCheckpointFailure(err error) {
	if err == nil {
		return
	}
	r.checkpointFailure.CompareAndSwap(nil, &replicaFailure{err: err})
	r.ready.Store(false)
}
