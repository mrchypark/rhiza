//go:build localhooks

package operator

import "os"

// The test-only localhooks build may use only the pinned, task-owned Floci
// endpoint and only when the caller opts in explicitly. Normal builds retain
// the production emulator rejection unconditionally.
func rejectGCSMutationEmulator() bool {
	endpoint := os.Getenv("STORAGE_EMULATOR_HOST")
	return endpoint != "" && (endpoint != "http://floci-gcs:4588" ||
		os.Getenv("RHIZA_LOCALHOOKS_ALLOW_FLOCI_GCS") != "1" ||
		os.Getenv("RHIZA_LOGICAL_AUTHORITY_FLOCI") != "1" ||
		os.Getenv("CI") != "true" || os.Getenv("GOOGLE_CLOUD_PROJECT") != "floci-local")
}
