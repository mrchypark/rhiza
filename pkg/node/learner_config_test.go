package node

import (
	"strings"
	"testing"

	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestLearnerBootstrapIdentity(t *testing.T) {
	cfg := types.ExecutionConfig{ClusterID: "cluster", NodeID: "new", AdminToken: "admin", EnableReconfiguration: true,
		Members: []quepaxa.Member{{ID: "a"}, {ID: "b"}}, Learner: &quepaxa.Member{ID: "new", PublicKey: quepaxa.PublicKey{1}}}
	if err := validateVoterMembership(&cfg); err != nil {
		t.Fatal(err)
	}
	before := voterIdentityFor(&cfg)
	cfg.Learner.PublicKey = quepaxa.PublicKey{2}
	after := voterIdentityFor(&cfg)
	if before.Membership != after.Membership || before.LearnerPublicKey == after.LearnerPublicKey {
		t.Fatal("learner credentials must be bound independently of bootstrap membership")
	}
	for _, change := range []func(*types.ExecutionConfig){
		func(c *types.ExecutionConfig) { c.EnableReconfiguration = false },
		func(c *types.ExecutionConfig) { c.Learner = &quepaxa.Member{ID: "a", PublicKey: quepaxa.PublicKey{3}} },
		func(c *types.ExecutionConfig) { c.Learner = &quepaxa.Member{ID: "new"} },
		func(c *types.ExecutionConfig) { c.Members = nil },
	} {
		copy := cfg
		change(&copy)
		if err := validateVoterMembership(&copy); err == nil {
			t.Fatal("accepted invalid learner configuration")
		}
	}
}

func TestEngineOwnedRemovalsRequiresExplicitMultiVoterReconfiguration(t *testing.T) {
	base := types.ExecutionConfig{
		ClusterID: "cluster", NodeID: "a", AdminToken: "admin", EnableReconfiguration: true,
		Members: []quepaxa.Member{{ID: "a"}, {ID: "b"}, {ID: "c"}},
	}
	base.EngineOwnedRemovals = true
	if err := validateVoterMembership(&base); err != nil {
		t.Fatalf("valid engine-owned configuration rejected: %v", err)
	}
	for name, mutate := range map[string]func(*types.ExecutionConfig){
		"reconfiguration disabled": func(c *types.ExecutionConfig) { c.EnableReconfiguration = false },
		"local mode":               func(c *types.ExecutionConfig) { c.Local = true },
		"single voter":             func(c *types.ExecutionConfig) { c.Members = c.Members[:1] },
	} {
		t.Run(name, func(t *testing.T) {
			config := base
			mutate(&config)
			err := validateVoterMembership(&config)
			if err == nil || !strings.Contains(err.Error(), "engine-owned removals require") {
				t.Fatalf("invalid engine-owned configuration error = %v", err)
			}
		})
	}
}
