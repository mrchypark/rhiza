package recovery

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/thanos-io/objstore"
)

// RequireEmptyArchiveScope rejects any prior archive, fork, checkpoint, or
// recovery state before a caller initializes a new archive HEAD.
func RequireEmptyArchiveScope(ctx context.Context, bucket objstore.Bucket, prefix string) error {
	if ctx == nil || bucket == nil || prefix == "" || prefix == "." || prefix == ".." || strings.HasPrefix(prefix, "../") ||
		prefix != strings.Trim(prefix, "/") || path.Clean(prefix) != prefix || strings.Contains(prefix, "\\") {
		return fmt.Errorf("recovery: invalid empty archive scope")
	}
	manager := NewManager(bucket, prefix, ArchiveConfigID)
	defer manager.Close()
	if err := manager.Load(ctx); err != nil {
		return fmt.Errorf("recovery: load archive before genesis: %w", err)
	}
	if _, exists := manager.HeadVersion(); exists || manager.Tip() != 0 {
		return fmt.Errorf("recovery: archive already has a HEAD")
	}

	for _, key := range []string{
		"fork/intent.json", "fork/result.json",
		"checkpoint/CURRENT", "checkpoint/PUBLISHER",
		"recovery/anchor.json", "recovery/generation.json", "recovery/successor.json",
	} {
		name := path.Join(prefix, key)
		exists, err := bucket.Exists(ctx, name)
		if err != nil {
			return fmt.Errorf("recovery: check prior archive state %q: %w", name, err)
		}
		if exists {
			return fmt.Errorf("recovery: prior archive state %q exists", name)
		}
	}

	for _, namespace := range []string{"archive", "fork", "checkpoint", "recovery"} {
		dir := path.Join(prefix, namespace) + "/"
		found := false
		if err := bucket.Iter(ctx, dir, func(string) error {
			found = true
			return nil
		}); err != nil {
			return fmt.Errorf("recovery: list prior archive state %q: %w", dir, err)
		}
		if found {
			return fmt.Errorf("recovery: prior archive state exists under %q", dir)
		}
	}
	return nil
}
