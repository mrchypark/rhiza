package fencing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// SourceMutationClient is the production local-supervisor mutation client.
// It serializes calls at the shared Bucket boundary and recovers saved tuples
// before accepting a new invocation; it does not identify calls by key or
// content.
type SourceMutationClient struct {
	mu       sync.Mutex
	state    *sourceRetryState
	client   *sourceMutationClient
	closed   bool
	closeErr error
}

// OpenSourceMutationClient creates the retry journal for this exact bound run.
// The caller must pass a fresh run-specific directory; existing producer state
// is never reset or adopted under a new identity.
func OpenSourceMutationClient(dir string, session *Session) (*SourceMutationClient, error) {
	if session == nil || dir == "" {
		return nil, fmt.Errorf("fencing: source mutation client requires a bound session and retry directory")
	}
	session.mu.Lock()
	if session.binding == nil || session.binding.Producer == nil || session.identity.WALIdentity == "" {
		session.mu.Unlock()
		return nil, fmt.Errorf("fencing: source mutation client requires a bound producer")
	}
	binding := session.binding.clone()
	session.mu.Unlock()
	state, err := createSourceRetryState(dir, binding)
	if err != nil {
		return nil, err
	}
	return &SourceMutationClient{state: state, client: &sourceMutationClient{state: state, authority: session}}, nil
}

// Upload consumes the exact caller bytes. OwnerStep is a durable per-invocation
// counter, not a key/content deduplication key.
func (c *SourceMutationClient) Upload(ctx context.Context, key string, condition MutationCondition, payload []byte) error {
	if ctx == nil {
		return fmt.Errorf("fencing: source mutation context is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usable(); err != nil {
		return err
	}
	if err := c.recoverPending(ctx); err != nil {
		return err
	}
	intent, err := c.nextIntent("upload", key, condition)
	if err != nil {
		return err
	}
	return c.client.Upload(ctx, intent, payload)
}

// Delete requires the exact generation selected by the caller. The ordinary
// key-only Bucket.Delete API cannot use this protected path.
func (c *SourceMutationClient) Delete(ctx context.Context, key string, generation int64) error {
	if ctx == nil {
		return fmt.Errorf("fencing: source mutation context is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usable(); err != nil {
		return err
	}
	if err := c.recoverPending(ctx); err != nil {
		return err
	}
	intent, err := c.nextIntent("delete", key, MutationCondition{Mode: MutationMatchGeneration, Generation: generation})
	if err != nil {
		return err
	}
	step, err := c.state.allocate(intent)
	if err != nil {
		return err
	}
	return c.client.settle(ctx, step)
}

// ArchiveGenesisStatus is an authenticated idempotent POST through the existing
// authority path. It does not allocate a mutation tuple or bypass Upload.
func (c *SourceMutationClient) ArchiveGenesisStatus(ctx context.Context, archiveKey string, configID uint) (ArchiveGenesisStatus, error) {
	if ctx == nil {
		return ArchiveGenesisStatus{}, fmt.Errorf("fencing: archive genesis status context is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.usable(); err != nil {
		return ArchiveGenesisStatus{}, err
	}
	return c.client.archiveGenesisStatus(ctx, archiveKey, configID)
}

func (c *SourceMutationClient) nextIntent(kind, key string, condition MutationCondition) (MutationIntent, error) {
	if key == "" {
		return MutationIntent{}, fmt.Errorf("fencing: source mutation key is empty")
	}
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	if err := c.state.usable(); err != nil {
		return MutationIntent{}, err
	}
	producer := c.state.data.Binding.Producer
	sequence := c.state.data.NextSequence
	return MutationIntent{
		ContractVersion: MutationIntentVersion,
		StoreScopeHash:  producer.StoreScopeHash,
		OwnerStep:       MutationOwnerStep{Owner: producer.ProducerUID, Workflow: producer.StageIncarnation, Step: fmt.Sprintf("bucket-%d", sequence), Attempt: 1},
		Condition:       condition,
		Kind:            kind,
		Key:             key,
	}, nil
}

func (c *SourceMutationClient) recoverPending(ctx context.Context) error {
	c.state.mu.Lock()
	data := c.state.data
	data.Steps = append([]sourceRetryStep(nil), c.state.data.Steps...)
	data.Stages = append([]sourceRetryStage(nil), c.state.data.Stages...)
	c.state.mu.Unlock()
	for _, step := range data.Steps {
		if step.Advanced {
			continue
		}
		intent := step.Entry.Intent
		if intent.Kind == "upload" {
			draft := uploadStageDraft(intent)
			stage, _, found, err := c.state.owner(draft)
			if err != nil || !found || stage.LocalPayload == nil {
				return errors.Join(ErrPending, err)
			}
			payload, err := c.state.readLocalPayload(*stage.LocalPayload)
			if err != nil {
				return err
			}
			err = c.client.Upload(ctx, draft, payload)
			if err != nil && !errors.Is(err, errMutationExcluded) && !errors.Is(err, errMutationRejected) && !errors.Is(err, ErrMutationConditionFailed) {
				return err
			}
			continue
		}
		if err := c.client.settle(ctx, step); err != nil && !errors.Is(err, errMutationExcluded) && !errors.Is(err, errMutationRejected) && !errors.Is(err, ErrMutationConditionFailed) {
			return err
		}
	}
	// A crash after durable payload staging but before tuple allocation still
	// leaves one exact invocation to recover. It is settled before a new counter
	// value can be assigned.
	for _, stage := range data.Stages {
		if stage.LocalPayload == nil {
			continue
		}
		allocated := false
		for _, step := range data.Steps {
			if step.Entry.Intent.OwnerStep == stage.Draft.OwnerStep {
				allocated = true
				break
			}
		}
		if allocated {
			continue
		}
		payload, err := c.state.readLocalPayload(*stage.LocalPayload)
		if err != nil {
			return err
		}
		if err := c.client.Upload(ctx, stage.Draft, payload); err != nil && !errors.Is(err, errMutationExcluded) && !errors.Is(err, errMutationRejected) && !errors.Is(err, ErrMutationConditionFailed) {
			return err
		}
	}
	return nil
}

func (c *SourceMutationClient) usable() error {
	if c == nil || c.state == nil || c.client == nil || c.closed {
		return fmt.Errorf("fencing: source mutation client is closed")
	}
	return nil
}

func (c *SourceMutationClient) DrainMutations(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if ctx == nil {
		return fmt.Errorf("fencing: source mutation drain context is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return c.closeErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.state == nil {
		return fmt.Errorf("fencing: source mutation retry state is unavailable")
	}
	c.closed = true
	c.state.mu.Lock()
	pending := false
	for _, step := range c.state.data.Steps {
		if !step.Advanced || step.Entry.Resolution == nil {
			pending = true
			break
		}
	}
	c.state.mu.Unlock()
	c.closeErr = c.state.Close()
	if pending {
		c.closeErr = errors.Join(c.closeErr, ErrPending)
	}
	return c.closeErr
}

func (c *SourceMutationClient) Close() error {
	return c.DrainMutations(context.Background())
}

// SourceRetryDirectory returns a run-scoped location beneath the node's data
// directory. The random supervisor RunID prevents accidental reuse by a later
// producer incarnation.
func SourceRetryDirectory(dataDir string, id Identity) (string, error) {
	if dataDir == "" || id.RunID == "" {
		return "", fmt.Errorf("fencing: complete source retry directory identity is required")
	}
	parent := filepath.Join(dataDir, "fencing-source")
	if err := os.MkdirAll(parent, 0700); err != nil {
		return "", err
	}
	return filepath.Join(parent, id.RunID), nil
}
