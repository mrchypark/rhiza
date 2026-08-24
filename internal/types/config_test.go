package types

import "testing"

func TestQuorumSizeIsStrictMajority(t *testing.T) {
	for members, want := range map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3} {
		config := ClusterConfig{Members: make([]NodeConfig, members)}
		if got := config.QuorumSize(); got != want {
			t.Fatalf("members=%d quorum=%d, want %d", members, got, want)
		}
	}
}
