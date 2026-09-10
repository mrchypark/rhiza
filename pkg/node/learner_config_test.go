package node

import (
	"github.com/mrchypark/rhiza/internal/types"
	"github.com/mrchypark/rhiza/pkg/quepaxa"
	"testing"
)

func TestLearnerBootstrapIdentity(t *testing.T) {
	cfg := types.ExecutionConfig{ClusterID: "cluster", NodeID: "new", AdminToken: "admin", EnableReconfiguration: true, Members: []quepaxa.Member{{ID: "a", Token: "a"}, {ID: "b", Token: "b"}}, Learner: &quepaxa.Member{ID: "new", Token: "new-token"}}
	if err := validateVoterMembership(&cfg); err != nil {
		t.Fatal(err)
	}
	before := voterIdentityFor(&cfg)
	cfg.Learner.Token = "changed"
	after := voterIdentityFor(&cfg)
	if before.Membership != after.Membership || before.LearnerTokenHash == after.LearnerTokenHash {
		t.Fatal("learner credentials must be bound independently of bootstrap membership")
	}
	for _, change := range []func(*types.ExecutionConfig){
		func(c *types.ExecutionConfig) { c.EnableReconfiguration = false },
		func(c *types.ExecutionConfig) { c.Learner = &quepaxa.Member{ID: "a", Token: "new-token"} },
		func(c *types.ExecutionConfig) { c.Learner = &quepaxa.Member{ID: "new", Token: "admin"} },
		func(c *types.ExecutionConfig) { c.Members = nil },
	} {
		copy := cfg
		change(&copy)
		if err := validateVoterMembership(&copy); err == nil {
			t.Fatal("accepted invalid learner configuration")
		}
	}
}
