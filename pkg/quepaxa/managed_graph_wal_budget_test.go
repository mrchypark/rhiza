package quepaxa

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestManagedGraphRecorderThreeReferenceBudget(t *testing.T) {
	var proposals [3]Proposal
	for i := range proposals {
		value := bytes.Repeat([]byte{byte(i + 1)}, 256<<10)
		proposals[i] = Proposal{ProposerID: "n1", Value: value, Hash: sha256.Sum256(value)}
	}
	state := ISR{Step: 6, FirstCurrent: &proposals[0], AggregateCurrent: &proposals[1], AggregatePrior: &proposals[2]}
	encoded := encodeRecorderEntry(1, state)
	if len(encoded) >= 1<<20 {
		t.Fatalf("three-reference WAL payload=%d", len(encoded))
	}
	decoded, err := decodeRecorderEntry(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range []*Proposal{decoded.State.FirstCurrent, decoded.State.AggregateCurrent, decoded.State.AggregatePrior} {
		if p == nil || !bytes.Equal(p.Value, proposals[i].Value) {
			t.Fatal("WAL reference lost")
		}
	}
	shared := encodeRecorderEntry(1, ISR{Step: 6, FirstCurrent: &proposals[0], AggregateCurrent: &proposals[0], AggregatePrior: &proposals[0]})
	if len(shared) >= 2*(256<<10) {
		t.Fatal("shared references were duplicated on wire")
	}
	// This deliberately tests the generic serializer's upper shape. Normal
	// recorder producers strip values and store them in separate proposals.
}
