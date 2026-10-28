package fencing

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// BootstrapMembersHash is the digest stored in BootstrapAssignment.MembershipHash.
// It deliberately hashes the full serialized member list, unlike the recovery
// membership identity digest, which is a separate fact.
func BootstrapMembersHash(members []quepaxa.Member) string {
	encoded, _ := json.Marshal(members)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
