//go:build !localhooks

package operator

import "os"

func rejectGCSMutationEmulator() bool { return os.Getenv("STORAGE_EMULATOR_HOST") != "" }
