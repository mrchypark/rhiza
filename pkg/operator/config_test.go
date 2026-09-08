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
