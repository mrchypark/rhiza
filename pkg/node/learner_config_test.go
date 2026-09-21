package node

import (
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"testing"
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
