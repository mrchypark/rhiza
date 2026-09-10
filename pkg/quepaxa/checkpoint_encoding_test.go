package quepaxa

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCheckpointSealLegacyEncodingRoundTripsByteIdentically(t *testing.T) {
	seal := CheckpointSeal{
		ConfigID: 7, Index: 11,
		RootHash: [32]byte{1}, StateHash: [32]byte{2}, PrefixHash: [32]byte{3},
		NextLeaderOrder: []NodeID{"a", "b"}, FollowingLeaderOrder: []NodeID{"b", "a"},
	}
	// This shape is the pre-generation-anchor checkpoint JSON encoding.
	legacy := struct {
		ConfigID             uint     `json:"config_id"`
		Index                Slot     `json:"index"`
		RootHash             [32]byte `json:"root_hash"`
		StateHash            [32]byte `json:"state_hash"`
		PrefixHash           [32]byte `json:"prefix_hash"`
		NextLeaderOrder      []NodeID `json:"next_leader_order"`
		FollowingLeaderOrder []NodeID `json:"following_leader_order,omitempty"`
	}{seal.ConfigID, seal.Index, seal.RootHash, seal.StateHash, seal.PrefixHash, seal.NextLeaderOrder, seal.FollowingLeaderOrder}
	payload, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	encoded := append(append([]byte(nil), checkpointSealMagic...), payload...)
	decoded, checkpoint, err := DecodeCheckpointSeal(encoded)
	if err != nil || !checkpoint {
		t.Fatalf("decode legacy checkpoint: checkpoint=%v err=%v", checkpoint, err)
	}
	reencoded, err := EncodeCheckpointSeal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(reencoded, encoded) {
		t.Fatalf("legacy checkpoint encoding changed\n got: %s\nwant: %s", reencoded, encoded)
	}
}
