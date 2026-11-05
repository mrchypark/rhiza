//go:build !rhiza_local_testhooks

package localtesthooks

// Proposal observers do not exist in normal binaries.
func SetProposalEntry(func([]byte)) func() { return func() {} }
func HitProposalEntry([]byte)              {}
