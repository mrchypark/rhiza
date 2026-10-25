package node

import (
	"context"
	"fmt"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// publishReconfigurationPrefix makes the fixed old-configuration prefix
// durable and publishes it through the Node's existing archive manager before
// QuePaxa proposes a membership terminal.
func (n *Node) publishReconfigurationPrefix(ctx context.Context, prefix quepaxa.ReconfigurationPrefix) error {
	if n == nil || n.core == nil || n.archive == nil {
		return fmt.Errorf("reconfiguration archive publisher is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := requireReconfigurationPrefix(n.core, prefix); err != nil {
		return err
	}
	if err := n.core.EnsureDurableThrough(ctx, prefix.Through); err != nil {
		return fmt.Errorf("make reconfiguration prefix durable: %w", err)
	}
	if err := requireReconfigurationPrefix(n.core, prefix); err != nil {
		return err
	}
	if err := n.archive.SyncPrefixThrough(ctx, reconfigurationArchiveSource{core: n.core}, prefix.Through); err != nil {
		return fmt.Errorf("publish reconfiguration prefix: %w", err)
	}
	if err := requireReconfigurationPrefix(n.core, prefix); err != nil {
		return err
	}
	return ctx.Err()
}

// reconfigurationArchiveSource exposes the implicit genesis prefix for a
// fresh core. The archive's empty head is rooted at the zero prefix, while
// Core stores only prefixes produced by decisions and therefore has no map
// entry for slot zero. Compacted cores still delegate the lookup unchanged.
type reconfigurationArchiveSource struct {
	core *quepaxa.Core
}

func (s reconfigurationArchiveSource) DecisionsFromBounded(from quepaxa.Slot, itemLimit, payloadByteLimit int) ([]quepaxa.DecidedValue, quepaxa.Slot, error) {
	return s.core.DecisionsFromBounded(from, itemLimit, payloadByteLimit)
}

func (s reconfigurationArchiveSource) PrefixHash(slot quepaxa.Slot) ([32]byte, bool) {
	if slot == 0 && s.core.CompactionFloor() == 0 {
		return [32]byte{}, true
	}
	return s.core.PrefixHash(slot)
}

func (s reconfigurationArchiveSource) Tip() quepaxa.Slot {
	return s.core.Tip()
}

func requireReconfigurationPrefix(core *quepaxa.Core, want quepaxa.ReconfigurationPrefix) error {
	if core == nil || want.Through == 0 {
		return fmt.Errorf("reconfiguration prefix is unavailable")
	}
	hash, ok := core.PrefixHash(want.Through)
	if !ok || quepaxa.ValueHash(hash) != want.PrefixHash {
		return fmt.Errorf("reconfiguration prefix hash changed at slot %d", want.Through)
	}
	return nil
}

// catchUpArchivePrefix imports only the already-published prefix before a
// known terminal. The peer handler remains responsible for accepting and
// acknowledging the terminal decision itself.
func (n *Node) catchUpArchivePrefix(ctx context.Context, core *quepaxa.Core, key quepaxa.TerminalGapKey) error {
	if n == nil || n.archive == nil || core == nil || !core.IsVoter() {
		return fmt.Errorf("archive continuation is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !core.PendingTerminalGap(key) {
		return nil
	}
	if key.Terminal <= 1 {
		return fmt.Errorf("pending terminal has no preceding archive prefix")
	}
	through := key.Terminal - 1
	if err := n.archive.LoadOnce(ctx); err != nil {
		return fmt.Errorf("refresh archive for pending terminal: %w", err)
	}
	if !core.PendingTerminalGap(key) {
		return nil
	}
	localTip := core.Tip()
	if localTip >= through {
		return nil
	}
	if through-localTip > 16 || n.archive.Tip() < through {
		return fmt.Errorf("published archive does not contain the bounded terminal prefix")
	}
	count := int(through - localTip)
	values, readTip, err := n.archive.DecisionsFrom(ctx, localTip+1, count)
	if err != nil {
		return fmt.Errorf("read pending terminal prefix: %w", err)
	}
	if readTip < through || len(values) != count {
		return fmt.Errorf("shared archive omitted pending terminal prefix")
	}
	for i, value := range values {
		if value.Slot != localTip+1+quepaxa.Slot(i) {
			return fmt.Errorf("shared archive returned a non-contiguous terminal prefix")
		}
		if err := types.ValidateExecutionPolicy(value.Value); err != nil {
			return fmt.Errorf("validate archived terminal prefix: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !core.PendingTerminalGap(key) {
		return nil
	}
	if err := core.AcceptCertifiedValues(values); err != nil {
		return fmt.Errorf("apply published terminal prefix: %w", err)
	}
	if err := core.EnsureDurableThrough(ctx, through); err != nil {
		return fmt.Errorf("persist published terminal prefix: %w", err)
	}
	return ctx.Err()
}
