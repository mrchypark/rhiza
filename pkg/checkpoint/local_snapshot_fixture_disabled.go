//go:build !rhiza_local_testhooks

package checkpoint

import "context"

func legacyLocalSnapshotUploadForTest(context.Context, *Manager, Source) (File, bool, error) {
	return File{}, false, nil
}
