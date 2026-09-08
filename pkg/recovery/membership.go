package recovery

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

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
		encoded, _ := json.Marshal([]string{string(member.ID), fmt.Sprintf("%x", sha256.Sum256([]byte(member.Token)))})
		encodedMembers = append(encodedMembers, string(encoded))
	}
	slices.Sort(encodedMembers)
	encoded, _ := json.Marshal(encodedMembers)
	return MembershipRecord{Version: 1, Cluster: cluster, Membership: fmt.Sprintf("%x", sha256.Sum256(encoded)), Durability: durability}
}
