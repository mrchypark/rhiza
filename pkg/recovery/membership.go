package recovery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

// MembershipRecordVersion 2 binds voter identity by public key. Version 1
// hashed the retired private peer token and is rejected on read.
const MembershipRecordVersion = 2

// MembershipRecord binds a generation's voting authority and durability mode.
// A mode change requires a new generation; an environment edit is not a
// durability-history transition.
type MembershipRecord struct {
	Version    int    `json:"version"`
	Cluster    string `json:"cluster"`
	Membership string `json:"membership"`
	Durability string `json:"durability"`
}

func NewMembershipRecord(cluster string, members []quepaxa.Member, durability string) MembershipRecord {
	if durability == "" {
		durability = "async"
	}
	encodedMembers := make([]string, 0, len(members))
	for _, member := range members {
		// Only public identity is bound. A voter's private peer token is never
		// part of a durable record or of any hash derived from one.
		encoded, _ := json.Marshal([]string{string(member.ID), hex.EncodeToString(member.PublicKey[:])})
		encodedMembers = append(encodedMembers, string(encoded))
	}
	slices.Sort(encodedMembers)
	encoded, _ := json.Marshal(encodedMembers)
	sum := sha256.Sum256(append([]byte("rhiza-membership-v2\x00"), encoded...))
	return MembershipRecord{Version: MembershipRecordVersion, Cluster: cluster, Membership: fmt.Sprintf("%x", sum), Durability: durability}
}
