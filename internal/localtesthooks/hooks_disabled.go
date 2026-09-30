//go:build !rhiza_local_testhooks

package localtesthooks

// Enabled is false when test boundary callbacks are compiled out.
const Enabled = false

// Hit is a production no-op. Boundary callbacks are only available in test
// binaries built with the rhiza_local_testhooks tag.
func Hit(string) {}
