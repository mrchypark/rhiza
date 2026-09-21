package operator

import (
	"context"
	"testing"
)

func TestNoPVCUsesEffectiveDataMount(t *testing.T) {
	sts := object{"spec": object{"template": object{"spec": object{"volumes": []any{
		object{"name": "ephemeral", "emptyDir": object{}}, object{"name": "host", "hostPath": object{"path": "/state"}},
	}}}}}
	c := object{"volumeMounts": []any{object{"name": "ephemeral", "mountPath": "/data"}}}
	if err := noPVC(sts, c, map[string]string{}); err != nil {
		t.Fatal(err)
	}
	c["volumeMounts"] = append(list(c["volumeMounts"]), object{"name": "host", "mountPath": "/data/db"})
	if err := noPVC(sts, c, map[string]string{}); err == nil {
		t.Fatal("persistent mount inside data directory accepted")
	}
	if err := noPVC(sts, c, map[string]string{"RHIZA_DATA_DIR": "/data/db"}); err == nil {
		t.Fatal("nested persistent mount accepted")
	}
	if err := noPVC(sts, c, map[string]string{"RHIZA_DATA_DIR": "/data/../state"}); err == nil {
		t.Fatal("path outside emptyDir accepted")
	}
}

func TestConfigurationRejectsUnresolvedStoreAndNestedState(t *testing.T) {
	c := &Controller{}
	_, err := c.environment(context.Background(), object{"env": []any{object{"name": "RHIZA_OBJSTORE_PREFIX", "valueFrom": object{"fieldRef": object{"fieldPath": "metadata.labels[archive]"}}}}})
	if err == nil {
		t.Fatal("unresolved archive namespace accepted")
	}
	env := map[string]string{"RHIZA_OBJSTORE_PROVIDER": "azure", "RHIZA_OBJSTORE_AZURE_CONNECTION_STRING": "AccountName=one;AccountKey=test-one"}
	c.StoreIdentity = StoreIdentity(env)
	env["RHIZA_OBJSTORE_AZURE_CONNECTION_STRING"] = "AccountName=two;AccountKey=test-two"
	if c.storeConfigMatches(env) == nil {
		t.Fatal("different connection-string account accepted")
	}
}

// An envFrom-provided scalar outlives a deletion, so the clearing override is
// the name-only entry the API server stores for an empty EnvVar.Value. The
// reader must resolve it to an empty value instead of rejecting it as an
// unresolved dynamic source.
func TestEnvironmentReadsNameOnlyClearingOverride(t *testing.T) {
	c := &Controller{}
	ctr := object{"env": []any{
		object{"name": "RHIZA_PEER_TOKEN"},
		object{"name": "RHIZA_CLUSTER_ID", "value": "target"},
	}}
	env, err := c.environment(context.Background(), ctr)
	if err != nil {
		t.Fatalf("name-only override rejected: %v", err)
	}
	if value, ok := env["RHIZA_PEER_TOKEN"]; !ok || value != "" {
		t.Fatalf("override resolved to %q ok=%t", value, ok)
	}
	if env["RHIZA_CLUSTER_ID"] != "target" {
		t.Fatalf("later entries dropped: %v", env)
	}
	// An unresolved dynamic source is still an error for any other key.
	if _, err := c.environment(context.Background(), object{"env": []any{object{"name": "RHIZA_OBJSTORE_PREFIX", "valueFrom": object{"fieldRef": object{"fieldPath": "metadata.labels[archive]"}}}}}); err == nil {
		t.Fatal("unresolved archive namespace accepted")
	}
}
