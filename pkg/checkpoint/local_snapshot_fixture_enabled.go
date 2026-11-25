//go:build rhiza_local_testhooks

package checkpoint

import (
	"context"
	"os"
)

// Reproduce the old conditional-upload code with an already existing block.
func legacyLocalSnapshotUploadForTest(ctx context.Context, m *Manager, source Source) (File, bool, error) {
	if os.Getenv("RHIZA_TEST_LEGACY_LOCAL_SNAPSHOT") != "1" {
		return File{}, false, nil
	}
	if _, err := m.uploadFile(ctx, source, 1, true, false, nil, nil); err != nil {
		return File{}, true, err
	}
	file, err := m.uploadFile(ctx, source, 1, true, false, nil, nil)
	return file, true, err
}
