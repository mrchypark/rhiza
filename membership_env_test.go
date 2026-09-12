package rhiza

import "testing"

func TestMembershipConfigFromEnv(t *testing.T) {
	t.Setenv("RHIZA_ENABLE_RECONFIGURATION", "true")
	t.Setenv("RHIZA_LEARNER", `{"node_id":"replacement","token":"replacement-token"}`)
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.EnableReconfiguration || cfg.Learner == nil || cfg.Learner.ID != "replacement" {
		t.Fatalf("membership config missing: %+v", cfg.Learner)
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
