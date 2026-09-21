package rhiza

import (
	"testing"

	"github.com/mrchypark/rhiza/pkg/quepaxa"
)

func TestMembershipConfigFromEnv(t *testing.T) {
	t.Setenv("RHIZA_ENABLE_RECONFIGURATION", "true")
	t.Setenv("RHIZA_LEARNER", `{"node_id":"replacement","public_key":"MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="}`)
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.EnableReconfiguration || cfg.Learner == nil || cfg.Learner.ID != "replacement" {
		t.Fatalf("membership config missing: %+v", cfg.Learner)
	}
	if cfg.Learner.PublicKey == (quepaxa.PublicKey{}) {
		t.Fatal("learner public key was dropped")
	}
	t.Setenv("RHIZA_LEARNER", `{"unknown":"field"}`)
	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("accepted unknown learner field")
	}
	t.Setenv("RHIZA_LEARNER", "")
	t.Setenv("RHIZA_ENABLE_RECONFIGURATION", "perhaps")
	if _, err := ConfigFromEnv(); err == nil {
		t.Fatal("accepted invalid feature flag")
	}
}
